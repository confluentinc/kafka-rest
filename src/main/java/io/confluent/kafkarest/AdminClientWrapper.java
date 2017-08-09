/*
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.kafkarest;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.ConfigResource;

import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.TreeSet;
import java.util.Vector;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import io.confluent.kafkarest.entities.Partition;
import io.confluent.kafkarest.entities.PartitionReplica;
import io.confluent.kafkarest.entities.Topic;
import io.confluent.rest.exceptions.RestServerErrorException;
import jersey.repackaged.com.google.common.collect.ImmutableList;


public class AdminClientWrapper {

  private AdminClient adminClient;
  private int initTimeOut;

  public AdminClientWrapper(KafkaRestConfig kafkaRestConfig) {
    Properties properties = new Properties(kafkaRestConfig.getAdminProperties());
    properties.put(KafkaRestConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaRestConfig.bootstrapBrokers());
    adminClient = AdminClient.create(properties);
    this.initTimeOut = kafkaRestConfig.getInt(KafkaRestConfig.KAFKACLIENT_INIT_TIMEOUT_CONFIG);
  }

  public List<Integer> getBrokerIds() {
    List<Integer> brokerIds = new Vector<>();
    DescribeClusterResult clusterResults = adminClient.describeCluster();
    try {
      Collection<Node>
          nodeCollection =
          clusterResults.nodes().get(initTimeOut, TimeUnit.MILLISECONDS);
      for (Node node : nodeCollection) {
        brokerIds.add(node.id());
      }
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      throw new RestServerErrorException(Errors.KAFKA_ERROR_MESSAGE, Errors
          .KAFKA_ERROR_ERROR_CODE, e);
    }
    return brokerIds;
  }

  public Collection<String> getTopicNames() {
    Collection<String> allTopics = null;
    try {
      allTopics =
          new TreeSet<>(adminClient.listTopics().names().get(initTimeOut, TimeUnit.MILLISECONDS));
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      throw new RestServerErrorException(Errors.KAFKA_ERROR_MESSAGE, Errors
          .KAFKA_ERROR_ERROR_CODE, e);
    }
    return allTopics;
  }

  public boolean topicExists(String topic) {
    try {
      Collection<String> allTopics =
          adminClient.listTopics().names().get(initTimeOut, TimeUnit.MILLISECONDS);
      return allTopics.contains(topic);
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      throw new RestServerErrorException(Errors.KAFKA_ERROR_MESSAGE, Errors
          .KAFKA_ERROR_ERROR_CODE, e);
    }
  }

  public Topic getTopic(String topicName) {
    Topic topic = null;
    try {
      if (topicExists(topicName)) {
        TopicDescription topicDescription = adminClient.describeTopics(
            ImmutableList.<String>of(topicName)).values().get(topicName)
            .get(initTimeOut, TimeUnit.MILLISECONDS);

        topic = buildTopic(topicName, topicDescription);
      }
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      throw new RestServerErrorException(Errors.KAFKA_ERROR_MESSAGE, Errors
          .KAFKA_ERROR_ERROR_CODE, e);
    }
    return topic;
  }

  public List<Partition> getTopicPartitions(String topicName) {
    List<Partition> partitions = null;
    try {
      TopicDescription topicDescription = adminClient.describeTopics(
          ImmutableList.<String>of(topicName)).values().get(topicName)
          .get(initTimeOut, TimeUnit.MILLISECONDS);

      partitions = buildPartitonsData(topicDescription.partitions(), null);
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      throw new RestServerErrorException(Errors.KAFKA_ERROR_MESSAGE, Errors
          .KAFKA_ERROR_ERROR_CODE, e);
    }
    return partitions;
  }

  public Partition getTopicPartition(String topicName, int partition) {
    List<Partition> partitions = null;
    try {
      TopicDescription topicDescription = adminClient.describeTopics(
          ImmutableList.<String>of(topicName)).values().get(topicName)
          .get(initTimeOut, TimeUnit.MILLISECONDS);

      partitions = buildPartitonsData(topicDescription.partitions(), partition);
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      throw new RestServerErrorException(Errors.KAFKA_ERROR_MESSAGE, Errors
          .KAFKA_ERROR_ERROR_CODE, e);
    }
    if (partitions.isEmpty()) {
      return null;
    }
    return partitions.get(0);
  }

  public boolean partitionExists(String topicName, int partition) {
    Topic topic = getTopic(topicName);
    return (partition >= 0 && partition < topic.getPartitions().size());
  }

  private Topic buildTopic(String topicName, TopicDescription topicDescription)
      throws InterruptedException, ExecutionException {
    Topic topic;
    List<Partition> partitions = buildPartitonsData(topicDescription.partitions(), null);

    ConfigResource topicResource = new ConfigResource(ConfigResource.Type.TOPIC, topicName);
    Config config = adminClient.describeConfigs(ImmutableList.<ConfigResource>of(topicResource))
        .values().get(topicResource).get();
    Properties topicProps = new Properties();
    for (ConfigEntry configEntry : config.entries()) {
      topicProps.put(configEntry.name(), configEntry.value());
    }
    topic = new Topic(topicName, topicProps, partitions);
    return topic;
  }

  private List<Partition> buildPartitonsData(
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
      p.setLeader(topicPartitionInfo.leader().id());
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

  public void shutdown() {
    adminClient.close();
  }
}
