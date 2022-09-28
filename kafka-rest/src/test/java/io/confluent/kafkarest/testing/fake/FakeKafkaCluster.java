/*
 * Copyright 2022 Confluent Inc.
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

package io.confluent.kafkarest.testing.fake;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Collections.emptySet;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.protobuf.ByteString;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import kafka.admin.AdminOperationException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.Deserializer;

public final class FakeKafkaCluster {
  private static final int NUM_BROKERS = 3;

  private final String clusterId = UUID.randomUUID().toString();
  private final Broker controller;
  private final ConcurrentMap<Integer, Broker> brokers = new ConcurrentHashMap<>();
  private final ConcurrentMap<String, Topic> topics = new ConcurrentHashMap<>();
  private final AtomicInteger nextReplica = new AtomicInteger(0);

  FakeKafkaCluster() {
    controller = new Broker(/* brokerId= */ 1, new ConfigStore(ConfigResource.Type.BROKER));
    brokers.put(controller.getBrokerId(), controller);
    for (int brokerId = 2; brokerId <= NUM_BROKERS; brokerId++) {
      brokers.put(brokerId, new Broker(brokerId, new ConfigStore(ConfigResource.Type.BROKER)));
    }
  }

  public String getClusterId() {
    return clusterId;
  }

  Broker getBroker(int brokerId) {
    Broker broker = brokers.get(brokerId);
    checkArgument(broker != null, "Broker %s does not exist.", brokerId);
    return broker;
  }

  Topic getTopic(String topicName) {
    Topic topic = topics.get(topicName);
    if (topic == null) {
      throw new UnknownTopicOrPartitionException(
          String.format("Topic %s doesn't exist.", topicName));
    }
    return topic;
  }

  public synchronized Topic createTopic(
      String topicName, Optional<Integer> numPartitions, Optional<Short> replicationFactor) {
    short actualReplicationFactor =
        replicationFactor.orElse(controller.getConfigs().getDefaultReplicationFactor());
    if (actualReplicationFactor > brokers.size()) {
      throw new AdminOperationException(
          String.format(
              "replication factor: %d larger than available brokers: %d",
              actualReplicationFactor, brokers.size()));
    }
    return createTopic(
        topicName,
        assignReplicas(
            numPartitions.orElse(controller.getConfigs().getNumPartitions()),
            replicationFactor.orElse(controller.getConfigs().getDefaultReplicationFactor())));
  }

  private ImmutableListMultimap<Integer, Integer> assignReplicas(
      int numPartitions, short replicationFactor) {
    ImmutableList<Integer> brokerIds = ImmutableList.sortedCopyOf(brokers.keySet());
    int firstReplica = nextReplica.getAndAdd(numPartitions * replicationFactor);
    ImmutableListMultimap.Builder<Integer, Integer> assignments = ImmutableListMultimap.builder();
    for (int partitionId = 1; partitionId <= numPartitions; partitionId++) {
      for (int replicaIdx = 0; replicaIdx < replicationFactor; replicaIdx++) {
        int brokerId = firstReplica + (partitionId - 1) * replicationFactor + replicaIdx;
        assignments.put(partitionId, brokerIds.get(brokerId % brokerIds.size()));
      }
    }
    return assignments.build();
  }

  public synchronized Topic createTopic(
      String topicName, ListMultimap<Integer, Integer> replicaAssignments) {
    ImmutableList.Builder<Partition> partitions = ImmutableList.builder();
    for (int partitionId : replicaAssignments.keySet()) {
      checkArgument(
          !replicaAssignments.get(partitionId).isEmpty(),
          "Partition %d needs at least one replica.",
          partitionId);
      Replica leader = new Replica(replicaAssignments.get(partitionId).get(0));
      List<Replica> followers =
          replicaAssignments.get(partitionId).stream()
              .skip(1)
              .map(Replica::new)
              .collect(toImmutableList());
      partitions.add(new Partition(topicName, partitionId, leader, followers));
    }
    Topic topic = new Topic(partitions.build());
    topics.put(topicName, topic);
    return topic;
  }

  public synchronized RecordMetadata produce(
      String topicName,
      int partitionId,
      List<Header> headers,
      Optional<ByteString> key,
      Optional<ByteString> value,
      Instant timestamp) {
    Partition partition = getTopic(topicName).getPartition(partitionId);
    Record record = new Record(headers, key, value, timestamp);
    int offset = partition.getLeader().addRecord(record);
    partition.getFollowers().forEach(replica -> replica.addRecord(record));
    return new RecordMetadata(
        new TopicPartition(topicName, partition.getPartitionId()),
        offset,
        0,
        timestamp.toEpochMilli(),
        key.map(ByteString::size).orElse(0),
        value.map(ByteString::size).orElse(0));
  }

  public <K, V> ConsumerRecord<K, V> getRecord(
      String topicName,
      int partitionId,
      long offset,
      Deserializer<K> keyDeserializer,
      Deserializer<V> valueDeserializer) {
    Record record =
        getTopic(topicName)
            .getPartition(partitionId)
            .getLeader()
            .getRecords()
            .get(Math.toIntExact(offset));
    return new ConsumerRecord<>(
        topicName,
        partitionId,
        offset,
        record.getTimestamp().toEpochMilli(),
        TimestampType.NO_TIMESTAMP_TYPE,
        record.getKey().map(ByteString::size).orElse(0),
        record.getValue().map(ByteString::size).orElse(0),
        keyDeserializer.deserialize(
            topicName, record.getKey().map(ByteString::toByteArray).orElse(null)),
        valueDeserializer.deserialize(
            topicName, record.getValue().map(ByteString::toByteArray).orElse(null)),
        new RecordHeaders(
            record.getHeaders().stream()
                .map(
                    header ->
                        new RecordHeader(
                            header.getKey(),
                            header.getValue().map(ByteString::toByteArray).orElse(null)))
                .collect(toImmutableList())),
        Optional.empty());
  }

  Cluster toClusterMetadata() {
    return new Cluster(
        clusterId,
        brokers.values().stream().map(Broker::toNode).collect(toImmutableList()),
        topics.values().stream()
            .flatMap(topic -> topic.getPartitions().stream())
            .map(partition -> partition.toPartitionInfo(this))
            .collect(toImmutableList()),
        emptySet(),
        emptySet());
  }
}
