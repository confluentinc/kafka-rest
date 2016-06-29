/**
 * Copyright 2016 Confluent Inc.
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

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Pattern;

/**
 * Tracks a consumer's state for a subscribed topics, including the underlying consumer and
 * consumed and committed offsets. It provides manual synchronization primitives to support
 * ConsumerWorkers protecting access to the state while they process a read request in their
 * processing loop.
 */
public class ConsumerSubscriptionState<KafkaK, KafkaV, ClientK, ClientV> implements ConsumerRebalanceListener {

  private static final Logger log = LoggerFactory.getLogger(ConsumerSubscriptionState.class);

  private final Lock lock = new ReentrantLock();
  private final Consumer<KafkaK, KafkaV> consumer;
  private final ConsumerInstanceId consumerId;
  private final Map<TopicPartition, OffsetAndMetadata> consumedOffsets;
  private final Map<TopicPartition, OffsetAndMetadata> committedOffsets;
  private Set<String> subscribedTopics;

  // The last read task on this topic that failed. Allows the next read to pick up where this one
  // left off, including accounting for response size limits
  private ConsumerReadTask failedTask;


  /**
   * Factory for creating ConsumerSubscriptionState instance using topic list
   * subscription strategy.
   * @param consumer KafkaConsumer object to which this SubscriptionState is related
   * @param topics collection of topics this consumer subscribes to
   * @param <KK> Kafka record key type
   * @param <KV> Kafka record value type
   * @param <CK> Client record key type
   * @param <CV> Client record value type
   * @return
   */
  public static <KK, KV, CK, CV>
  ConsumerSubscriptionState<KK, KV, CK, CV> subscribeByTopicList(Consumer<KK,KV> consumer,
                                                                 ConsumerInstanceId consumerId,
                                                                 Collection<String> topics) {
    ConsumerSubscriptionState<KK, KV, CK, CV> subscriptionState =
            new ConsumerSubscriptionState<KK, KV, CK, CV>(consumer, consumerId);
    consumer.subscribe(topics, subscriptionState);
    return subscriptionState;
  }


  /**
   * Factory for creating ConsumerSubscriptionState instance using pattern
   * subscription strategy.
   * @param consumer KafkaConsumer object to which this SubscriptionState is related
   * @param regex regular expression that represents topic pattern this consumer subscribes to
   * @param <KK> Kafka record key type
   * @param <KV> Kafka record value type
   * @param <CK> Client record key type
   * @param <CV> Client record value type
   * @return
   */
  public static <KK, KV, CK, CV>
  ConsumerSubscriptionState<KK, KV, CK, CV> subscribeByTopicRegex(KafkaConsumer<KK,KV> consumer,
                                                                  ConsumerInstanceId consumerId,
                                                                  String regex) {
    ConsumerSubscriptionState<KK, KV, CK, CV> subscriptionState =
            new ConsumerSubscriptionState<KK, KV, CK, CV>(consumer, consumerId);
    Pattern pattern = Pattern.compile(regex);
    consumer.subscribe(pattern, subscriptionState);
    return subscriptionState;
  }

  private ConsumerSubscriptionState(Consumer<KafkaK, KafkaV> consumer, ConsumerInstanceId consumerId) {
    this.consumer = consumer;
    this.consumerId = consumerId;
    this.subscribedTopics = new HashSet<>();
    this.consumedOffsets = new HashMap<TopicPartition, OffsetAndMetadata>();
    this.committedOffsets = new HashMap<TopicPartition, OffsetAndMetadata>();
  }

  public void lock() {
    lock.lock();
  }

  public void unlock() {
    lock.unlock();
  }

  /**
   * Checks whether this subscription contains #topic
   * @param topic topic
   * @return
   */
  public boolean isSubscribed(String topic) {
    boolean result = subscribedTopics.contains(topic);
    return result || (subscribedTopics = consumer.subscription()).contains(topic);
  }

  public Consumer<KafkaK, KafkaV> getConsumer() {
    return consumer;
  }

  public Map<TopicPartition, OffsetAndMetadata> getConsumedOffsets() {
    return consumedOffsets;
  }

  public Map<TopicPartition, OffsetAndMetadata> getCommittedOffsets() {
    return committedOffsets;
  }

  public ConsumerReadTask clearFailedTask() {
    ConsumerReadTask t = failedTask;
    failedTask = null;
    return t;
  }

  public void setFailedTask(ConsumerReadTask failedTask) {
    this.failedTask = failedTask;
  }

  @Override
  public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
    for (TopicPartition tp: partitions) {
      log.info("Consumer: {} Revoked: {}-{}", consumerId.toString(), tp.topic(), tp.partition());
      consumedOffsets.remove(tp);
      committedOffsets.remove(tp);
    }
  }

  @Override
  public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
    for (TopicPartition tp: partitions) {
      log.info("Consumer: {} Assigned: {}-{}", consumerId.toString(), tp.topic(), tp.partition());
    }
  }
}
