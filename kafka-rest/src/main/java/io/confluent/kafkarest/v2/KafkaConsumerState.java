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

import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;

import io.confluent.kafkarest.ConsumerInstanceId;
import io.confluent.kafkarest.ConsumerRecordAndSize;
import io.confluent.kafkarest.KafkaRestConfig;
import io.confluent.kafkarest.entities.v2.ConsumerAssignmentRequest;
import io.confluent.kafkarest.entities.v2.ConsumerCommittedRequest;
import io.confluent.kafkarest.entities.v2.ConsumerCommittedResponse;
import io.confluent.kafkarest.entities.v2.ConsumerOffsetCommitRequest;
import io.confluent.kafkarest.entities.v2.ConsumerSeekToOffsetRequest;
import io.confluent.kafkarest.entities.v2.ConsumerSeekToRequest;
import io.confluent.kafkarest.entities.v2.ConsumerSubscriptionRecord;
import io.confluent.kafkarest.entities.TopicPartitionOffset;
import io.confluent.kafkarest.entities.v2.TopicPartitionOffsetMetadata;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.Vector;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Pattern;
import javax.ws.rs.InternalServerErrorException;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;

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

  private Queue<ConsumerRecord<KafkaKeyT, KafkaValueT>> consumerRecords = new ArrayDeque<>();

  volatile long expiration;
  private ReentrantLock lock;

  KafkaConsumerState(
      KafkaRestConfig config,
      ConsumerInstanceId instanceId,
      Consumer<KafkaKeyT, KafkaValueT> consumer
  ) {
    this.config = config;
    this.instanceId = instanceId;
    this.consumer = consumer;
    this.expiration = config.getTime().milliseconds()
                      + config.getInt(KafkaRestConfig.CONSUMER_INSTANCE_TIMEOUT_MS_CONFIG);
    this.lock = new ReentrantLock();
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
  public List<TopicPartitionOffset> commitOffsets(
      String async,
      ConsumerOffsetCommitRequest offsetCommitRequest
  ) {
    lock.lock();
    try {
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
        for (TopicPartitionOffsetMetadata t : offsetCommitRequest.getOffsets()) {
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
    } finally {
      lock.unlock();
    }
  }

  /**
   * Seek to the first offset for each of the given partitions.
   */
  public void seekToBeginning(ConsumerSeekToRequest seekToRequest) {
    lock.lock();
    try {
      if (seekToRequest != null) {
        Vector<TopicPartition> topicPartitions = new Vector<TopicPartition>();

        for (io.confluent.kafkarest.entities.v2.TopicPartition t : seekToRequest.getPartitions()) {
          topicPartitions.add(new TopicPartition(t.getTopic(), t.getPartition()));
        }
        consumer.seekToBeginning(topicPartitions);
      }
    } finally {
      lock.unlock();
    }
  }

  /**
   * Seek to the last offset for each of the given partitions.
   */
  public void seekToEnd(ConsumerSeekToRequest seekToRequest) {
    lock.lock();
    try {
      if (seekToRequest != null) {
        Vector<TopicPartition> topicPartitions = new Vector<TopicPartition>();

        for (io.confluent.kafkarest.entities.v2.TopicPartition t : seekToRequest.getPartitions()) {
          topicPartitions.add(new TopicPartition(t.getTopic(), t.getPartition()));
        }
        consumer.seekToEnd(topicPartitions);
      }
    } finally {
      lock.unlock();
    }
  }

  /**
   * Overrides the fetch offsets that the consumer will use on the next poll(timeout).
   */
  public void seekToOffset(ConsumerSeekToOffsetRequest seekToOffsetRequest) {
    lock.lock();
    try {
      if (seekToOffsetRequest != null) {
        for (TopicPartitionOffsetMetadata t : seekToOffsetRequest.getOffsets()) {
          TopicPartition topicPartition = new TopicPartition(t.getTopic(), t.getPartition());
          consumer.seek(topicPartition, t.getOffset());
        }

      }
    } finally {
      lock.unlock();
    }
  }

  /**
   * Manually assign a list of partitions to this consumer.
   */
  public void assign(ConsumerAssignmentRequest assignmentRequest) {
    lock.lock();
    try {
      if (assignmentRequest != null) {
        Vector<TopicPartition> topicPartitions = new Vector<TopicPartition>();

        for (io.confluent.kafkarest.entities.v2.TopicPartition t
            : assignmentRequest.getPartitions()) {
          topicPartitions.add(new TopicPartition(t.getTopic(), t.getPartition()));
        }
        consumer.assign(topicPartitions);
      }
    } finally {
      lock.unlock();
    }
  }

  /**
   * Close the consumer,
   */

  public void close() {
    lock.lock();
    try {
      if (consumer != null) {
        consumer.close();
      }
      // Marks this state entry as no longer valid because the consumer group is being destroyed.
      consumer = null;
    } finally {
      lock.unlock();
    }
  }


  /**
   * Subscribe to the given list of topics to get dynamically assigned partitions.
   */
  public void subscribe(ConsumerSubscriptionRecord subscription) {
    if (subscription == null) {
      return;
    }

    lock.lock();
    try {
      if (consumer != null) {
        if (subscription.getTopics() != null) {
          consumer.subscribe(subscription.getTopics());
        } else if (subscription.getTopicPattern() != null) {
          Pattern topicPattern = Pattern.compile(subscription.getTopicPattern());
          NoOpOnRebalance noOpOnRebalance = new NoOpOnRebalance();
          consumer.subscribe(topicPattern, noOpOnRebalance);
        }
      }
    } finally {
      lock.unlock();
    }
  }

  /**
   * Unsubscribe from topics currently subscribed with subscribe(Collection).
   */
  public void unsubscribe() {
    lock.lock();
    try {
      if (consumer != null) {
        consumer.unsubscribe();
      }
    } finally {
      lock.unlock();
    }
  }

  /**
   * Get the current list of topics subscribed.
   */
  public java.util.Set<String> subscription() {
    java.util.Set<String> currSubscription = null;
    lock.lock();
    try {
      if (consumer != null) {
        currSubscription = consumer.subscription();
      }
    } finally {
      lock.unlock();
    }
    return currSubscription;
  }

  /**
   * Get the set of partitions currently assigned to this consumer.
   */
  public java.util.Set<TopicPartition> assignment() {
    java.util.Set<TopicPartition> currAssignment = null;
    lock.lock();
    try {
      if (consumer != null) {
        currAssignment = consumer.assignment();
      }
    } finally {
      lock.unlock();
    }
    return currAssignment;
  }


  /**
   * Get the last committed offset for the given partition (whether the commit happened by
   * this process or another).
   */
  public ConsumerCommittedResponse committed(ConsumerCommittedRequest request) {
    lock.lock();
    try {
      Vector<TopicPartitionOffsetMetadata> offsets = new Vector<>();
      if (consumer != null) {
        for (io.confluent.kafkarest.entities.v2.TopicPartition t : request.getPartitions()) {
          TopicPartition partition = new TopicPartition(t.getTopic(), t.getPartition());
          OffsetAndMetadata offsetMetadata = consumer.committed(partition);
          if (offsetMetadata != null) {
            offsets.add(
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
      return new ConsumerCommittedResponse(offsets);
    } finally {
      lock.unlock();
    }
  }

  /**
   * Returns the beginning offset of the {@code topic} {@code partition}.
   */
  long getBeginningOffset(String topic, int partition) {
    lock.lock();
    try {
      if (consumer == null) {
        throw new IllegalStateException("KafkaConsumerState has been closed.");
      }

      Map<TopicPartition, Long> response =
          consumer.beginningOffsets(singletonList(new TopicPartition(topic, partition)));

      if (response.size() != 1) {
        throw new InternalServerErrorException(
            String.format("Expected one offset, but got %d instead.", response.size()));
      }

      return response.values().stream().findAny().get();
    } finally {
      lock.unlock();
    }
  }

  /**
   * Returns the end offset of the {@code topic} {@code partition}.
   */
  long getEndOffset(String topic, int partition) {
    lock.lock();
    try {
      if (consumer == null) {
        throw new IllegalStateException("KafkaConsumerState has been closed.");
      }

      Map<TopicPartition, Long> response =
          consumer.endOffsets(singletonList(new TopicPartition(topic, partition)));

      if (response.size() != 1) {
        throw new InternalServerErrorException(
            String.format("Expected one offset, but got %d instead.", response.size()));
      }

      return response.values().stream().findAny().get();
    } finally {
      lock.unlock();
    }
  }

  /**
   * Returns the earliest offset whose timestamp is greater than or equal to the given {@code
   * timestamp} in the {@code topic} {@code partition}, or empty if such offset does not exist.
   */
  Optional<Long> getOffsetForTime(String topic, int partition, Instant timestamp) {
    lock.lock();
    try {
      if (consumer == null) {
        throw new IllegalStateException("KafkaConsumerState has been closed.");
      }

      Map<TopicPartition, OffsetAndTimestamp> response =
          consumer.offsetsForTimes(
              singletonMap(new TopicPartition(topic, partition), timestamp.toEpochMilli()));

      if (response.size() != 1) {
        throw new InternalServerErrorException(
            String.format("Expected one offset, but got %d instead.", response.size()));
      }

      return response.values().stream()
          .filter(Objects::nonNull)
          .findAny()
          .map(OffsetAndTimestamp::offset);
    } finally {
      lock.unlock();
    }
  }

  public boolean expired(long nowMs) {
    return expiration <= nowMs;
  }

  public void updateExpiration() {
    this.expiration = config.getTime().milliseconds()
                      + config.getInt(KafkaRestConfig.CONSUMER_INSTANCE_TIMEOUT_MS_CONFIG);
  }

  public KafkaRestConfig getConfig() {
    return config;
  }

  public void setConfig(KafkaRestConfig config) {
    this.config = config;
  }


  ConsumerRecord<KafkaKeyT, KafkaValueT> peek() {
    return consumerRecords.peek();
  }

  boolean hasNext() {
    lock.lock();
    try {
      if (hasNextCached()) {
        return true;
      }
      // If none are available, try checking for any records already fetched by the consumer.
      getOrCreateConsumerRecords();

      return hasNextCached();
    } finally {
      lock.unlock();
    }
  }

  boolean hasNextCached() {
    return !consumerRecords.isEmpty();
  }

  ConsumerRecord<KafkaKeyT, KafkaValueT> next() {
    return consumerRecords.poll();
  }

  /**
   * Initiate poll(0) request to retrieve consumer records that are available immediately, or return
   * the existing
   * consumer records if the records have not been fully consumed by client yet. Must be
   * invoked with the lock held, i.e. after startRead().
   */
  private void getOrCreateConsumerRecords() {
    consumerRecords = new ArrayDeque<>();
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

