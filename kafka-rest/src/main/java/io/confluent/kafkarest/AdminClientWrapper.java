/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.kafkarest;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.admin.ListConsumerGroupsOptions;
import org.apache.kafka.clients.admin.DescribeConsumerGroupsOptions;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.ConfigResource;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeSet;
import java.util.Vector;
import java.util.concurrent.TimeUnit;

import io.confluent.kafkarest.entities.Partition;
import io.confluent.kafkarest.entities.PartitionReplica;
import io.confluent.kafkarest.entities.Topic;

public class AdminClientWrapper {

  private final AdminClient adminClient;
  private final int initTimeOut;
  private final int requestTimeOut;

  public AdminClientWrapper(KafkaRestConfig kafkaRestConfig, AdminClient adminClient) {
    this.adminClient = adminClient;
    this.initTimeOut = kafkaRestConfig.getInt(KafkaRestConfig.KAFKACLIENT_INIT_TIMEOUT_CONFIG);
    this.requestTimeOut = kafkaRestConfig.getInt(
        KafkaRestConfig.KAFKACLIENT_REQUEST_TIMEOUT_CONFIG);
  }

  static Properties adminProperties(KafkaRestConfig kafkaRestConfig) {
    Properties properties = new Properties();
    properties.putAll(kafkaRestConfig.getAdminProperties());
    properties.put(KafkaRestConfig.BOOTSTRAP_SERVERS_CONFIG,
        RestConfigUtils.bootstrapBrokers(kafkaRestConfig));
    return properties;
  }

  public List<Integer> getBrokerIds() throws Exception {
    List<Integer> brokerIds = new Vector<>();
    DescribeClusterResult clusterResults = adminClient.describeCluster();
    Collection<Node> nodeCollection =
        clusterResults.nodes().get(initTimeOut, TimeUnit.MILLISECONDS);
    for (Node node : nodeCollection) {
      brokerIds.add(node.id());
    }
    return brokerIds;
  }

  public Collection<String> getTopicNames() throws Exception {
    return new TreeSet<>(
        adminClient.listTopics().names().get(initTimeOut, TimeUnit.MILLISECONDS));
  }

  public boolean topicExists(String topic) throws Exception {
    return getTopicNames().contains(topic);
  }

  public Topic getTopic(String topicName) throws Exception {
    if (topicExists(topicName)) {
      TopicDescription topicDescription = getTopicDescription(topicName);

      return buildTopic(topicName, topicDescription);
    } else {
      return null;
    }
  }

  public List<Partition> getTopicPartitions(String topicName) throws Exception {
    TopicDescription topicDescription = getTopicDescription(topicName);
    return buildPartitionsData(topicDescription.partitions(), null);
  }

  public Partition getTopicPartition(String topicName, int partition) throws Exception {
    TopicDescription topicDescription = getTopicDescription(topicName);
    List<Partition> partitions = buildPartitionsData(topicDescription.partitions(), partition);
    if (partitions.isEmpty()) {
      return null;
    }
    return partitions.get(0);
  }

  public boolean partitionExists(String topicName, int partition) throws Exception {
    Topic topic = getTopic(topicName);
    return (partition >= 0 && partition < topic.getPartitions().size());
  }

  public Collection<ConsumerGroupListing> listConsumerGroups() throws Exception {
    return adminClient.listConsumerGroups(new ListConsumerGroupsOptions()
        .timeoutMs(requestTimeOut)).all().get(requestTimeOut, TimeUnit.MILLISECONDS);
  }

  public Map<String, ConsumerGroupDescription> describeConsumerGroups(
          Collection<String> groupIds) throws Exception {
    return adminClient.describeConsumerGroups(groupIds,
        new DescribeConsumerGroupsOptions().timeoutMs(requestTimeOut))
        .all().get(requestTimeOut, TimeUnit.MILLISECONDS);
  }

  private Topic buildTopic(String topicName, TopicDescription topicDescription) throws Exception {
    List<Partition> partitions = buildPartitionsData(topicDescription.partitions(), null);

    ConfigResource topicResource = new ConfigResource(ConfigResource.Type.TOPIC, topicName);
    Config config = adminClient.describeConfigs(
        Collections.unmodifiableList(Collections.singletonList(topicResource))
    ).values().get(topicResource).get();
    Properties topicProps = new Properties();
    for (ConfigEntry configEntry : config.entries()) {
      topicProps.put(configEntry.name(), configEntry.value());
    }
    return new Topic(topicName, topicProps, partitions);
  }

  private List<Partition> buildPartitionsData(
      List<TopicPartitionInfo> partitions,
      Integer partitionsFilter
  ) {
    List<Partition> partitionList = new Vector<>();
    for (TopicPartitionInfo topicPartitionInfo : partitions) {

      if (partitionsFilter != null && !partitionsFilter.equals(topicPartitionInfo.partition())) {
        continue;
      }

      Partition p = new Partition();
      p.setPartition(topicPartitionInfo.partition());
      Node partitionLeader = topicPartitionInfo.leader();
      int leaderId = partitionLeader != null ? partitionLeader.id() : -1;
      p.setLeader(leaderId);
      List<PartitionReplica> partitionReplicas = new Vector<>();

      for (Node replicaNode : topicPartitionInfo.replicas()) {
        partitionReplicas.add(new PartitionReplica(replicaNode.id(),
            replicaNode.id() == p.getLeader(), topicPartitionInfo.isr().contains(replicaNode)
        ));
      }
      p.setReplicas(partitionReplicas);
      partitionList.add(p);
    }
    return partitionList;
  }

  private TopicDescription getTopicDescription(String topicName) throws Exception {
    return adminClient.describeTopics(Collections.singletonList(topicName))
        .values().get(topicName).get(initTimeOut, TimeUnit.MILLISECONDS);
  }

  public void shutdown() {
    adminClient.close();
  }
}
