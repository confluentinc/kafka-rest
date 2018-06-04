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

import kafka.cluster.Broker;
import kafka.cluster.EndPoint;
import kafka.utils.ZkUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.security.JaasUtils;
import scala.collection.JavaConverters;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.TreeSet;
import java.util.Vector;
import java.util.concurrent.TimeUnit;

import io.confluent.kafkarest.entities.BrokerEntity;
import io.confluent.kafkarest.entities.EndPointEntity;
import io.confluent.kafkarest.entities.NodeState;
import io.confluent.kafkarest.entities.Partition;
import io.confluent.kafkarest.entities.PartitionReplica;
import io.confluent.kafkarest.entities.Topic;

public class AdminClientWrapper {

  private AdminClient adminClient;
  private KafkaRestConfig kafkaRestConfig;
  private int initTimeOut;

  public AdminClientWrapper(KafkaRestConfig kafkaRestConfig, AdminClient adminClient) {
    this.adminClient = adminClient;
    this.initTimeOut = kafkaRestConfig.getInt(KafkaRestConfig.KAFKACLIENT_INIT_TIMEOUT_CONFIG);
  }

  public static Properties adminProperties(KafkaRestConfig kafkaRestConfig) {
    this.kafkaRestConfig = kafkaRestConfig;
    Properties properties = new Properties();
    properties.putAll(kafkaRestConfig.getAdminProperties());
    properties.put(KafkaRestConfig.BOOTSTRAP_SERVERS_CONFIG,
        RestConfigUtils.bootstrapBrokers(kafkaRestConfig));
    return properties;
  }

  /**
   * <p>Check if broker is available</p>
   *
   * @param brokerId - broker ID for check
   * @return true if brokerInfo by ID not null
   */
  public NodeState getBrokerState(Integer brokerId) {
    DescribeClusterResult clusterResults = adminClient.describeCluster();
    try {
      Collection<Node> nodeCollection =
          clusterResults.nodes().get(initTimeOut, TimeUnit.MILLISECONDS);
      for (Node eachNode: nodeCollection) {
        if (brokerId.equals(eachNode.id())) {
          return new NodeState(true);
        }
      }
      return new NodeState(false);
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      throw new RestServerErrorException(Errors.KAFKA_ERROR_MESSAGE,
              Errors.KAFKA_ERROR_ERROR_CODE, e
      );
    }
  }

  /**
   * <p>Get broker metadata</p>
   *
   * @param brokerId - broker ID
   * @return metadata about broker by ID or null if broker not found
   */
  public BrokerEntity getBroker(int brokerId) {
    final ZkUtils zkUtils = getZkUtils();
    for (Broker broker :
            JavaConverters.seqAsJavaListConverter(zkUtils.getAllBrokersInCluster()).asJava()) {
      if (broker.id() == brokerId) {
        List<EndPointEntity> endpoints = new ArrayList<>();
        for (EndPoint endPoint :
                JavaConverters.seqAsJavaListConverter(broker.endPoints()).asJava()) {
          endpoints.add(new EndPointEntity(endPoint.securityProtocol().name,
                  endPoint.host(),
                  endPoint.port()));
        }
        return new BrokerEntity(broker.id(), endpoints);
      }
    }
    return BrokerEntity.empty();
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
    Collection<String> allTopics = null;
    allTopics = new TreeSet<>(
        adminClient.listTopics().names().get(initTimeOut, TimeUnit.MILLISECONDS));
    return allTopics;
  }

  public boolean topicExists(String topic) throws Exception {
    Collection<String> allTopics = getTopicNames();
    return allTopics.contains(topic);
  }

  public Topic getTopic(String topicName) throws Exception {
    Topic topic = null;
    if (topicExists(topicName)) {
      TopicDescription topicDescription = getTopicDescription(topicName);

      topic = buildTopic(topicName, topicDescription);
    }
    return topic;
  }

  public List<Partition> getTopicPartitions(String topicName) throws Exception {
    TopicDescription topicDescription = getTopicDescription(topicName);
    List<Partition> partitions = buildPartitonsData(topicDescription.partitions(), null);
    return partitions;
  }

  public Partition getTopicPartition(String topicName, int partition) throws Exception {
    TopicDescription topicDescription = getTopicDescription(topicName);
    List<Partition> partitions = buildPartitonsData(topicDescription.partitions(), partition);
    if (partitions.isEmpty()) {
      return null;
    }
    return partitions.get(0);
  }

  public boolean partitionExists(String topicName, int partition) throws Exception {
    Topic topic = getTopic(topicName);
    return (partition >= 0 && partition < topic.getPartitions().size());
  }

  private Topic buildTopic(String topicName, TopicDescription topicDescription) throws Exception {
    List<Partition> partitions = buildPartitonsData(topicDescription.partitions(), null);

    ConfigResource topicResource = new ConfigResource(ConfigResource.Type.TOPIC, topicName);
    Config config = adminClient.describeConfigs(
        Collections.unmodifiableList(Arrays.asList(topicResource))
    ).values().get(topicResource).get();
    Properties topicProps = new Properties();
    for (ConfigEntry configEntry : config.entries()) {
      topicProps.put(configEntry.name(), configEntry.value());
    }
    Topic topic = new Topic(topicName, topicProps, partitions);
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
    return adminClient.describeTopics(Collections.unmodifiableList(Arrays.asList(topicName)))
        .values().get(topicName).get(initTimeOut, TimeUnit.MILLISECONDS);
  }

  private ZkUtils getZkUtils() {
    final String zookeeperConnect =
            kafkaRestConfig.getString(KafkaRestConfig.ZOOKEEPER_CONNECT_CONFIG);
    final int sessionTimeout =
            kafkaRestConfig.getInt(KafkaRestConfig.KAFKACLIENT_ZK_SESSION_TIMEOUT_MS_CONFIG);
    final int defaultZkTimeout = 3000;
    return ZkUtils.apply(zookeeperConnect,
            (sessionTimeout != 0) ? sessionTimeout : defaultZkTimeout,
            (sessionTimeout != 0) ? sessionTimeout : defaultZkTimeout,
            JaasUtils.isZkSecurityEnabled());
  }

  public void shutdown() {
    adminClient.close();
  }
}
