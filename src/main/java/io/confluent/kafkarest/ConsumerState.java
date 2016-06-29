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

import io.confluent.kafkarest.entities.TopicPartitionOffset;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Vector;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Tracks all the state for a consumer. This class is abstract in order to support multiple
 * serialization formats. Implementations must provide deserializers and a method to convert Kafka
 * MessageAndMetadata<K,V> values to ConsumerRecords that can be returned to the client (including
 * translation if the decoded Kafka consumer type and AbstractConsumerRecord types differ).
 */
public abstract class ConsumerState<KafkaK, KafkaV, ClientK, ClientV>
    implements Comparable<ConsumerState>, AutoCloseable {

  private Consumer<KafkaK, KafkaV> consumer;
  protected KafkaRestConfig config;
  private ConsumerInstanceId instanceId;
  // new consumer is not multithreaded and can have only one subscription state
  private ConsumerSubscriptionState<KafkaK, KafkaV, ClientK, ClientV> subscription;
  private long expiration;
  // A lock on the ConsumerState allows concurrent readTopic calls, but allows
  // commitOffsets to safely lock the entire state in order to get correct information about all
  // the topic/stream's current offset state. All operations on individual TopicStates must be
  // synchronized at that level as well (so, e.g., readTopic may modify a single TopicState, but
  // only needs read access to the ConsumerState).
  private ReentrantLock lock;

  public ConsumerState(KafkaRestConfig config, ConsumerInstanceId instanceId,
                       Properties consumerProperties, ConsumerManager.ConsumerFactory consumerFactory) {
    this.config = config;
    this.instanceId = instanceId;
    // create new consumer
    this.consumer = consumerFactory.createConsumer(
            consumerProperties, getKeyDeserializer(), getValueDeserializer());
    this.subscription = null;
    this.expiration = config.getTime().milliseconds() +
                      config.getInt(KafkaRestConfig.CONSUMER_INSTANCE_TIMEOUT_MS_CONFIG);
    this.lock = new ReentrantLock();
  }

  public ConsumerInstanceId getId() {
    return instanceId;
  }

  /**
   * Gets the key deserializer for the Kafka consumer.
   */
  protected abstract Deserializer<KafkaK> getKeyDeserializer();

  /**
   * Gets the value deserializer for the Kafka consumer.
   */
  protected abstract Deserializer<KafkaV> getValueDeserializer();

  /**
   * Converts a ConsumerRecord using the Kafka deserializer types into a AbstractConsumerRecord using the
   * client's requested types. While doing so, computes the approximate size of the message in
   * bytes, which is used to track the approximate total payload size for consumer read responses to
   * determine when to trigger the response.
   */
  public abstract ConsumerRecordAndSize<ClientK, ClientV> convertConsumerRecord(
      ConsumerRecord<KafkaK, KafkaV> msg);

  /**
   * Start a read on the given topic, enabling a read lock on this ConsumerState and a full lock on
   * the ConsumerSubscriptionState.
   */
  public void startRead(ConsumerSubscriptionState<KafkaK, KafkaV, ClientK, ClientV> subscriptionState) {
    lock.lock();
    subscriptionState.lock();
  }

  /**
   * Finish a read request, releasing the lock on the ConsumerSubscriptionState and the read lock on this
   * ConsumerState.
   */
  public void finishRead(ConsumerSubscriptionState<KafkaK, KafkaV, ClientK, ClientV> subscriptionState) {
    subscriptionState.unlock();
    lock.unlock();
  }

  public List<TopicPartitionOffset> commitOffsets() {
    lock.lock();
    try {
      List<TopicPartitionOffset> result = getOffsets(true);
      return result;
    } finally {
      lock.unlock();
    }
  }

  @Override
  public void close() {
    lock.lock();
    try {
      // interrupt consumer poll request
      consumer.wakeup();
      consumer.close();
      // Marks this state entry as no longer valid because the consumer group is being destroyed.
      consumer = null;
      subscription = null;
    } finally {
      lock.unlock();
    }
  }

  public boolean expired(long nowMs) {
    return expiration <= nowMs;
  }

  public void updateExpiration() {
    this.expiration = config.getTime().milliseconds() +
                      config.getInt(KafkaRestConfig.CONSUMER_INSTANCE_TIMEOUT_MS_CONFIG);
  }

  public long untilExpiration(long nowMs) {
    return this.expiration - nowMs;
  }

  public KafkaRestConfig getConfig() {
    return config;
  }

  public void setConfig(KafkaRestConfig config) {
    this.config = config;
  }

  @Override
  public int compareTo(ConsumerState o) {
    if (this.expiration < o.expiration) {
      return -1;
    } else if (this.expiration == o.expiration) {
      return 0;
    } else {
      return 1;
    }
  }

  public ConsumerSubscriptionState<KafkaK, KafkaV, ClientK, ClientV> tryToSubscribe(String topic) {
    // Try getting the topic only using the read lock
    lock.lock();
    try {
      if (subscription == null) {
        subscription = ConsumerSubscriptionState
                .subscribeByTopicList(consumer, instanceId, Collections.singletonList(topic));
        consumer.subscription();
        return subscription;
      } else {
        // check whether the topic can be consumed
        if (subscription.isSubscribed(topic)) {
          return subscription;
        } else {
          throw Errors.consumerAlreadySubscribedException();
        }
      }
    } finally {
      lock.unlock();
    }
  }

  /**
   * Gets a list of TopicPartitionOffsets describing the current state of consumer offsets, possibly
   * updating the committed offset record. This method is not synchronized.
   *
   * @param doCommit if true, updates committed offsets to be the same as the consumed
   *                            offsets.
   */
  private List<TopicPartitionOffset> getOffsets(boolean doCommit) {
    List<TopicPartitionOffset> result = new Vector<TopicPartitionOffset>();
    ConsumerSubscriptionState<KafkaK, KafkaV, ClientK, ClientV> state = subscription;
    state.lock();
    try {
      for (Map.Entry<TopicPartition, OffsetAndMetadata> entry: state.getConsumedOffsets().entrySet()) {
        Integer partition = entry.getKey().partition();
        Long offset = entry.getValue().offset();
        Long committedOffset = null;
        if (doCommit) {

          // committed offsets are next offsets to be fetched
          // after the commit so we are increasing them by 1.
          OffsetAndMetadata newMetadata = new OffsetAndMetadata(
            entry.getValue().offset() + 1,
            entry.getValue().metadata());

          state.getCommittedOffsets().put(entry.getKey(), newMetadata);
          committedOffset = offset;
        } else {
          OffsetAndMetadata committed = state.getCommittedOffsets().get(entry.getKey());
          committedOffset = committed == null ? null : committed.offset();
        }
        result.add(new TopicPartitionOffset(entry.getKey().topic(), partition, offset,
                (committedOffset == null ? -1 : committedOffset)));
      }

      if (doCommit) {
        consumer.commitSync(state.getCommittedOffsets());
      }
    } finally {
      state.unlock();
    }
    return result;
  }



}