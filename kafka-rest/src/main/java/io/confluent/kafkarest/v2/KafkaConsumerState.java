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

package io.confluent.kafkarest.v2;

import java.util.Set;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Queue;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.regex.Pattern;

import io.confluent.kafkarest.ConsumerInstanceId;
import io.confluent.kafkarest.ConsumerRecordAndSize;
import io.confluent.kafkarest.KafkaRestConfig;
import io.confluent.kafkarest.entities.ConsumerAssignmentRequest;
import io.confluent.kafkarest.entities.ConsumerCommittedRequest;
import io.confluent.kafkarest.entities.ConsumerCommittedResponse;
import io.confluent.kafkarest.entities.ConsumerOffsetCommitRequest;
import io.confluent.kafkarest.entities.ConsumerSeekToOffsetRequest;
import io.confluent.kafkarest.entities.ConsumerSeekToRequest;
import io.confluent.kafkarest.entities.ConsumerSubscriptionRecord;
import io.confluent.kafkarest.entities.TopicPartitionOffset;
import io.confluent.kafkarest.entities.TopicPartitionOffsetMetadata;

/**
 * Tracks all the state for a consumer. This class is abstract in order to support multiple
 * serialization formats. Implementations must provide decoders and a method to convert
 * {@code KafkaMessageAndMetadata<K,V>} values to ConsumerRecords that can be returned to the client
 * (including translation if the decoded Kafka consumer type and ConsumerRecord types differ).
 */
public abstract class KafkaConsumerState<KafkaKeyT, KafkaValueT, ClientKeyT, ClientValueT> {

  private KafkaRestConfig config;
  private ConsumerInstanceId instanceId;
  private Consumer<KafkaKeyT, KafkaValueT> consumer;

  private final Queue<ConsumerRecord<KafkaKeyT, KafkaValueT>> consumerRecords = new ArrayDeque<>();

  volatile long expiration;

  public KafkaConsumerState(
      KafkaRestConfig config,
      ConsumerInstanceId instanceId,
      Consumer<KafkaKeyT, KafkaValueT> consumer
  ) {
    this.config = config;
    this.instanceId = instanceId;
    this.consumer = consumer;
    this.expiration = config.getTime().milliseconds()
                      + config.getInt(KafkaRestConfig.CONSUMER_INSTANCE_TIMEOUT_MS_CONFIG);
  }

  public ConsumerInstanceId getId() {
    return instanceId;
  }

  /**
   * Converts a MessageAndMetadata using the Kafka decoder types into a ConsumerRecord using the
   * client's requested types. While doing so, computes the approximate size of the message in
   * bytes, which is used to track the approximate total payload size for consumer read responses to
   * determine when to trigger the response.
   */
  public abstract ConsumerRecordAndSize<ClientKeyT, ClientValueT> createConsumerRecord(
      ConsumerRecord<KafkaKeyT, KafkaValueT> msg
  );

  /**
   * Commit the given list of offsets
   */
  public synchronized List<TopicPartitionOffset> commitOffsets(
      String async,
      ConsumerOffsetCommitRequest offsetCommitRequest
  ) {
    // If no offsets are given, then commit all the records read so far
    if (offsetCommitRequest == null) {
      if (async == null) {
        consumer.commitSync();
      } else {
        consumer.commitAsync();
      }
    } else {
      Map<TopicPartition, OffsetAndMetadata> offsetMap =
          new HashMap<TopicPartition, OffsetAndMetadata>();

      //commit each given offset
      for (TopicPartitionOffsetMetadata t : offsetCommitRequest.offsets) {
        if (t.getMetadata() == null) {
          offsetMap.put(
              new TopicPartition(t.getTopic(), t.getPartition()),
              new OffsetAndMetadata(t.getOffset() + 1)
          );
        } else {
          offsetMap.put(
              new TopicPartition(t.getTopic(), t.getPartition()),
              new OffsetAndMetadata(t.getOffset() + 1, t.getMetadata())
          );
        }

      }
      consumer.commitSync(offsetMap);
    }
    List<TopicPartitionOffset> result = new Vector<TopicPartitionOffset>();
    return result;
  }

  /**
   * Seek to the first offset for each of the given partitions.
   */
  public synchronized void seekToBeginning(ConsumerSeekToRequest seekToRequest) {
    if (seekToRequest != null) {
      Vector<TopicPartition> topicPartitions = new Vector<TopicPartition>();

      for (io.confluent.kafkarest.entities.TopicPartition t : seekToRequest.partitions) {
        topicPartitions.add(new TopicPartition(t.getTopic(), t.getPartition()));
      }
      consumer.seekToBeginning(topicPartitions);
    }
  }

  /**
   * Seek to the last offset for each of the given partitions.
   */
  public synchronized void seekToEnd(ConsumerSeekToRequest seekToRequest) {
    if (seekToRequest != null) {
      Vector<TopicPartition> topicPartitions = new Vector<TopicPartition>();

      for (io.confluent.kafkarest.entities.TopicPartition t : seekToRequest.partitions) {
        topicPartitions.add(new TopicPartition(t.getTopic(), t.getPartition()));
      }
      consumer.seekToEnd(topicPartitions);
    }
  }

  /**
   * Overrides the fetch offsets that the consumer will use on the next poll(timeout).
   */
  public synchronized void seekToOffset(ConsumerSeekToOffsetRequest seekToOffsetRequest) {
    if (seekToOffsetRequest != null) {
      for (TopicPartitionOffsetMetadata t : seekToOffsetRequest.offsets) {
        TopicPartition topicPartition = new TopicPartition(t.getTopic(), t.getPartition());
        consumer.seek(topicPartition, t.getOffset());
      }
    }
  }

  /**
   * Manually assign a list of partitions to this consumer.
   */
  public synchronized void assign(ConsumerAssignmentRequest assignmentRequest) {
    if (assignmentRequest != null) {
      Vector<TopicPartition> topicPartitions = new Vector<TopicPartition>();

      for (io.confluent.kafkarest.entities.TopicPartition t : assignmentRequest.partitions) {
        topicPartitions.add(new TopicPartition(t.getTopic(), t.getPartition()));
      }
      consumer.assign(topicPartitions);
    }
  }

  /**
   * Close the consumer,
   */
  public synchronized void close() {
    if (consumer != null) {
      consumer.close();
    }
    // Marks this state entry as no longer valid because the consumer group is being destroyed.
    consumer = null;
  }

  /**
   * Subscribe to the given list of topics to get dynamically assigned partitions.
   */
  public synchronized void subscribe(ConsumerSubscriptionRecord subscription) {
    if (subscription == null) {
      return;
    }

    if (consumer != null) {
      if (subscription.topics != null) {
        consumer.subscribe(subscription.topics);
      } else if (subscription.getTopicPattern() != null) {
        Pattern topicPattern = Pattern.compile(subscription.getTopicPattern());
        NoOpOnRebalance noOpOnRebalance = new NoOpOnRebalance();
        consumer.subscribe(topicPattern, noOpOnRebalance);
      }
    }
  }

  /**
   * Unsubscribe from topics currently subscribed with subscribe(Collection).
   */
  public synchronized void unsubscribe() {
    if (consumer != null) {
      consumer.unsubscribe();
    }
  }

  /**
   * Get the current list of topics subscribed.
   */
  public synchronized java.util.Set<String> subscription() {
    Set<String> currSubscription = null;
    if (consumer != null) {
      currSubscription = consumer.subscription();
    }
    return currSubscription;
  }

  /**
   * Get the set of partitions currently assigned to this consumer.
   */
  public synchronized Set<TopicPartition> assignment() {
    Set<TopicPartition> currAssignment = null;
    if (consumer != null) {
      currAssignment = consumer.assignment();
    }
    return currAssignment;
  }

  /**
   * Get the last committed offset for the given partition (whether the commit happened by
   * this process or another).
   */
  public synchronized ConsumerCommittedResponse committed(ConsumerCommittedRequest request) {
    ConsumerCommittedResponse response = new ConsumerCommittedResponse();
    response.offsets = new Vector<TopicPartitionOffsetMetadata>();
    if (consumer != null) {
      for (io.confluent.kafkarest.entities.TopicPartition t : request.partitions) {
        TopicPartition partition = new TopicPartition(t.getTopic(), t.getPartition());
        OffsetAndMetadata offsetMetadata = consumer.committed(partition);
        if (offsetMetadata != null) {
          response.offsets.add(
              new TopicPartitionOffsetMetadata(
                  partition.topic(),
                  partition.partition(),
                  offsetMetadata.offset(),
                  offsetMetadata.metadata()
              )
          );
        }
      }
    }
    return response;
  }

  public synchronized boolean expired(long nowMs) {
    return expiration <= nowMs;
  }

  public synchronized void updateExpiration() {
    this.expiration = config.getTime().milliseconds()
                      + config.getInt(KafkaRestConfig.CONSUMER_INSTANCE_TIMEOUT_MS_CONFIG);
  }

  public synchronized KafkaRestConfig getConfig() {
    return config;
  }

  public synchronized void setConfig(KafkaRestConfig config) {
    this.config = config;
  }

  synchronized ConsumerRecord<KafkaKeyT, KafkaValueT> peek() {
    return consumerRecords.peek();
  }

  synchronized boolean hasNext() {
    if (hasNextCached()) {
      return true;
    }
    // If none are available, try checking for any records already fetched by the consumer.
    getOrCreateConsumerRecords();

    return hasNextCached();
  }

  synchronized boolean hasNextCached() {
    return !consumerRecords.isEmpty();
  }

  synchronized ConsumerRecord<KafkaKeyT, KafkaValueT> next() {
    return consumerRecords.poll();
  }

  /**
   * Initiate poll(0) request to retrieve consumer records that are available immediately, or return
   * the existing
   * consumer records if the records have not been fully consumed by client yet. Must be
   * invoked with the lock held, i.e. after startRead().
   */
  private synchronized void getOrCreateConsumerRecords() {
    ConsumerRecords<KafkaKeyT, KafkaValueT> polledRecords = consumer.poll(0);
    //drain the iterator and buffer to list
    for (ConsumerRecord<KafkaKeyT, KafkaValueT> consumerRecord : polledRecords) {
      consumerRecords.add(consumerRecord);
    }
  }

  private class NoOpOnRebalance implements ConsumerRebalanceListener {

    public NoOpOnRebalance() {
    }

    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
    }

    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
    }
  }
}

