/**
 * Copyright 2015 Confluent Inc.
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
 **/
package io.confluent.kafkarest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.Vector;

import javax.ws.rs.InternalServerErrorException;

import io.confluent.kafkarest.entities.Partition;
import io.confluent.kafkarest.entities.PartitionReplica;
import io.confluent.kafkarest.entities.Topic;
import io.confluent.rest.exceptions.RestNotFoundException;
import kafka.admin.AdminUtils;
import kafka.api.LeaderAndIsr;
import kafka.cluster.Broker;
import kafka.utils.ZkUtils;
import scala.Option;
import scala.collection.JavaConversions;
import scala.collection.Map;
import scala.collection.Seq;
import scala.math.Ordering;

/**
 * Observes metadata about the Kafka cluster.
 */
public class MetadataObserver {

  private static final Logger log = LoggerFactory.getLogger(ConsumerWorker.class);

  private ZkUtils zkUtils;

  public MetadataObserver(KafkaRestConfig config, ZkUtils zkUtils) {
    this.zkUtils = zkUtils;
  }

  public List<Integer> getBrokerIds() {
    Seq<Broker> brokers = zkUtils.getAllBrokersInCluster();
    List<Integer> brokerIds = new Vector<Integer>(brokers.size());
    for (Broker broker : JavaConversions.asJavaCollection(brokers)) {
      brokerIds.add(broker.id());
    }
    return brokerIds;
  }

  private Broker getBrokerById(final int brokerId) {
    Option<Broker> broker = zkUtils.getBrokerInfo(brokerId);

    if (broker.isDefined()) {
      return broker.get();
    } else {
      throw Errors.LeaderNotAvailableException();
    }
  }

  public Broker getLeader(final String topicName, final int partitionId) {
    return getBrokerById(getLeaderId(topicName, partitionId));
  }

  public Collection<String> getTopicNames() {
    Seq<String> topicNames = zkUtils.getAllTopics().sorted(Ordering.String$.MODULE$);
    return JavaConversions.asJavaCollection(topicNames);
  }

  public List<Topic> getTopics() {
    try {
      Seq<String> topicNames = zkUtils.getAllTopics().sorted(Ordering.String$.MODULE$);
      return getTopicsData(topicNames);
    } catch (RestNotFoundException e) {
      throw new InternalServerErrorException(e);
    }
  }

  public boolean topicExists(String topicName) {
    List<Topic> topics = getTopics();
    for (Topic topic : topics) {
      if (topic.getName().equals(topicName)) {
        return true;
      }
    }
    return false;
  }

  public Topic getTopic(String topicName) {
    List<Topic> topics =
        getTopicsData(JavaConversions.asScalaBuffer(Arrays.asList(topicName)));
    return (topics.isEmpty() ? null : topics.get(0));
  }

  private List<Topic> getTopicsData(Seq<String> topicNames) {
    Map<String, Map<Object, Seq<Object>>> topicPartitions =
        zkUtils.getPartitionAssignmentForTopics(topicNames);
    List<Topic> topics = new Vector<Topic>(topicNames.size());
    // Admin utils only supports getting either 1 or all topic configs. These per-topic overrides
    // shouldn't be common, so we just grab all of them to keep this simple
    Map<String, Properties> configs = AdminUtils.fetchAllTopicConfigs(zkUtils);
    for (String topicName : JavaConversions.asJavaCollection(topicNames)) {
      Map<Object, Seq<Object>> partitionMap = topicPartitions.get(topicName).get();
      List<Partition> partitions = extractPartitionsFromZKData(partitionMap, topicName, null);
      if (partitions.size() == 0) {
        continue;
      }
      Option<Properties> topicConfigOpt = configs.get(topicName);
      Properties topicConfigs =
          topicConfigOpt.isEmpty() ? new Properties() : topicConfigOpt.get();
      Topic topic = new Topic(topicName, topicConfigs, partitions);
      topics.add(topic);
    }
    return topics;
  }

  public List<Partition> getTopicPartitions(String topic) {
    return getTopicPartitions(topic, null);
  }

  public boolean partitionExists(String topicName, int partition) {
    Topic topic = getTopic(topicName);
    return (partition >= 0 && partition < topic.getPartitions().size());
  }

  public Partition getTopicPartition(String topic, int partition) {
    List<Partition> partitions = getTopicPartitions(topic, partition);
    if (partitions.isEmpty()) {
      return null;
    }
    return partitions.get(0);
  }

  private List<Partition> getTopicPartitions(String topic, Integer partitions_filter) {
    Map<String, Map<Object, Seq<Object>>> topicPartitions = zkUtils.getPartitionAssignmentForTopics(
        JavaConversions.asScalaBuffer(Arrays.asList(topic)));
    Map<Object, Seq<Object>> parts = topicPartitions.get(topic).get();
    return extractPartitionsFromZKData(parts, topic, partitions_filter);
  }

  public int getLeaderId(final String topicName, final int partitionId) {
    final List<Partition> partitions = getTopicPartitions(topicName);

    if (partitions.size() == 0) {
      throw Errors.topicNotFoundException();
    }

    for (final Partition partition : partitions) {
      if (partition.getPartition() == partitionId) {
        return partition.getLeader();
      }
    }

    throw Errors.partitionNotFoundException();
  }

  private List<Partition> extractPartitionsFromZKData(
      Map<Object, Seq<Object>> parts, String topic, Integer partitions_filter) {
    List<Partition> partitions = new Vector<Partition>();
    java.util.Map<Object, Seq<Object>> partsJava = JavaConversions.mapAsJavaMap(parts);
    for (java.util.Map.Entry<Object, Seq<Object>> part : partsJava.entrySet()) {
      int partId = (Integer) part.getKey();
      if (partitions_filter != null && partitions_filter != partId) {
        continue;
      }

      Partition p = new Partition();
      p.setPartition(partId);
      LeaderAndIsr leaderAndIsr =
          zkUtils.getLeaderAndIsrForPartition(topic, partId).get();
      p.setLeader(leaderAndIsr.leader());
      scala.collection.immutable.Set<Integer> isr = leaderAndIsr.isr().toSet();
      List<PartitionReplica> partReplicas = new Vector<PartitionReplica>();
      for (Object brokerObj : JavaConversions.asJavaCollection(part.getValue())) {
        int broker = (Integer) brokerObj;
        PartitionReplica
            r =
            new PartitionReplica(broker, (leaderAndIsr.leader() == broker), isr.contains(broker));
        partReplicas.add(r);
      }
      p.setReplicas(partReplicas);
      partitions.add(p);
    }
    return partitions;
  }

  public void shutdown() {
    log.debug("Shutting down MetadataObserver");
  }
}
