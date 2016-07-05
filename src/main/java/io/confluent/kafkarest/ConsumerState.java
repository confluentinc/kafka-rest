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
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Pattern;

/**
 * Tracks all the state for a consumer. This class is abstract in order to support multiple
 * serialization formats. Implementations must provide deserializers and a method to convert Kafka
 * MessageAndMetadata<K,V> values to ConsumerRecords that can be returned to the client (including
 * translation if the decoded Kafka consumer type and AbstractConsumerRecord types differ).
 */
public abstract class ConsumerState<KafkaK, KafkaV, ClientK, ClientV>
    implements Comparable<ConsumerState>, AutoCloseable, ConsumerRebalanceListener {

  private static final Logger log = LoggerFactory.getLogger(ConsumerState.class);

  private Consumer<KafkaK, KafkaV> consumer;
  private AtomicBoolean isSubscribed;

  private Map<TopicPartition, OffsetAndMetadata> consumedOffsets;
  private Map<TopicPartition, OffsetAndMetadata> committedOffsets;

  // Queue to store extra fetched records by KafkaConsumer.
  // In order to improve performance we don't
  private Queue<ConsumerRecord<KafkaK, KafkaV>> recordsQueue;

  protected KafkaRestConfig config;
  private ConsumerInstanceId instanceId;

  private long expiration;
  // Since KafkaConsumer is single-threaded we need to guarantee that only
  // one thread works with it at a time.
  private ReentrantLock lock;
  private ReentrantLock heartbeatLock;
  // KafkaConsumer should perform regular heartbeat operations in order
  // to stay in a group.
  private long nextHeartbeatTime;
  private final long heartbeatDelay;
  private ConsumerHeartbeatThread heartbeatThread;

  // The last read task on this topic that failed. Allows the next read to pick up where this one
  // left off, including accounting for response size limits
  private ConsumerReadTask failedTask;


  public ConsumerState(KafkaRestConfig config, ConsumerInstanceId instanceId,
                       Properties consumerProperties, ConsumerManager.ConsumerFactory consumerFactory) {
    this.config = config;
    this.instanceId = instanceId;
    // create new consumer
    this.consumer = consumerFactory.createConsumer(
            consumerProperties, getKeyDeserializer(), getValueDeserializer());
    this.expiration = config.getTime().milliseconds() +
                      config.getInt(KafkaRestConfig.CONSUMER_INSTANCE_TIMEOUT_MS_CONFIG);
    this.lock = new ReentrantLock();
    this.heartbeatLock = new ReentrantLock();
    this.recordsQueue = new LinkedList<>();

    this.isSubscribed = new AtomicBoolean(false);
    this.consumedOffsets = new HashMap<TopicPartition, OffsetAndMetadata>();
    this.committedOffsets = new HashMap<TopicPartition, OffsetAndMetadata>();

    //TODO do not hardcode default session.timeout.ms
    final int defaultSessionTimeoutMs = 30000;
    String consumerSessionTimeout = consumerProperties
      .getProperty(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG);
    if (consumerSessionTimeout == null) {
      // heartbeat thread will perform poll each
      // with frequency of 1/4 session timeout.
      this.heartbeatDelay = defaultSessionTimeoutMs / 4;
    } else {
      this.heartbeatDelay = Integer.valueOf(consumerSessionTimeout);
    }

    this.nextHeartbeatTime = 0;
    this.heartbeatThread = new ConsumerHeartbeatThread();
    heartbeatThread.start();
  }


  public ConsumerInstanceId getId() {
    return instanceId;
  }

  public Consumer<KafkaK, KafkaV> getConsumer() {
    return consumer;
  }

  /**
   *
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
  public void startRead() {
    lock.lock();
    heartbeatLock.lock();
  }

  /**
   * Finish a read request, releasing the lock on the ConsumerSubscriptionState and the read lock on this
   * ConsumerState.
   */
  public void finishRead() {
    // after finishing read change heartbeat time
    this.nextHeartbeatTime = config.getTime().milliseconds() + heartbeatDelay;
    lock.unlock();
    heartbeatLock.unlock();
  }

  public boolean readStarted() {
    return lock.isLocked();
  }

  public ConsumerReadTask clearFailedTask() {
    ConsumerReadTask t = failedTask;
    failedTask = null;
    return t;
  }

  public void setFailedTask(ConsumerReadTask failedTask) {
    this.failedTask = failedTask;
  }

  public Map<TopicPartition, OffsetAndMetadata> getConsumedOffsets() {
    return consumedOffsets;
  }

  public Map<TopicPartition, OffsetAndMetadata> getCommittedOffsets() {
    return committedOffsets;
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

  /**
   * To keep a Consumer subscribed the poll must be invoked periodically
   * to send heartbeats to the kafka back-end
   * This method should not block for a long period.
   */
  public void sendHeartbeat() {
    // if the lock cannot be acquired it means that the read operation
    // is being occurred and there is no reason to perform poll.
    if (consumer != null  // consumer is null after close()
      && isSubscribed.get()
      && config.getTime().milliseconds() >= nextHeartbeatTime
      && heartbeatLock.tryLock()) {
      try {
        log.info("Consumer {} sends heartbeat.", instanceId.getInstance());
        ConsumerRecords<KafkaK, KafkaV> records = consumer.poll(0);

        // change next heartbeat time before processing records
        this.nextHeartbeatTime = config.getTime().milliseconds() + heartbeatDelay;

        for (ConsumerRecord<KafkaK, KafkaV> record: records) {
          recordsQueue.add(record);
        }
      } finally {
        heartbeatLock.unlock();
      }
    }
  }

  public long getNextHeartbeatTime() {
    return nextHeartbeatTime;
  }

  public Queue<ConsumerRecord<KafkaK, KafkaV>> queue() {
    return recordsQueue;
  }


  @Override
  public void close() {
    lock.lock();
    heartbeatLock.lock();
    try {
      heartbeatThread.shutdown();
      // interrupt consumer poll request
      consumer.wakeup();
      consumer.close();
      // Marks this state entry as no longer valid because the consumer group is being destroyed.
      consumer = null;
    } finally {
      lock.unlock();
      heartbeatLock.unlock();
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

  public boolean isSubscribed() {
    return isSubscribed.get();
  }

  public Set<String> getSubscribedTopics() {
    if (!isSubscribed.get()) {
      return Collections.emptySet();
    } else {
      lock.lock();
      try {
        return consumer.subscription();
      } finally {
        lock.unlock();
      }
    }
  }

  public void tryToSubscribeByTopicList(Collection<String> topics) {
    lock.lock();
    try {
      if (!isSubscribed.get()) {
        consumer.subscribe(topics, this);
        isSubscribed.set(true);
      } else {
        throw Errors.consumerAlreadySubscribedException();
      }
    } finally {
      lock.unlock();
    }
  }

  public void tryToSubscribeByTopicRegex(String regex) {
    lock.lock();
    try {
      if (!isSubscribed.get()) {
        consumer.subscribe(Pattern.compile(regex), this);
        isSubscribed.set(true);
      } else {
        throw Errors.consumerAlreadySubscribedException();
      }
    } finally {
      lock.unlock();
    }
  }

  public void unsubscribe() {
    lock.lock();
    try {
      clearFailedTask();
      consumer.unsubscribe();
      isSubscribed.set(false);
      committedOffsets = null;
      consumedOffsets = null;
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
    lock.lock();
    try {
      for (Map.Entry<TopicPartition, OffsetAndMetadata> entry: consumedOffsets.entrySet()) {
        Integer partition = entry.getKey().partition();
        Long offset = entry.getValue().offset();
        Long committedOffset = null;
        if (doCommit) {

          // committed offsets are next offsets to be fetched
          // after the commit so we are increasing them by 1.
          OffsetAndMetadata newMetadata = new OffsetAndMetadata(
            entry.getValue().offset() + 1,
            entry.getValue().metadata());

          committedOffsets.put(entry.getKey(), newMetadata);
          committedOffset = offset;
        } else {
          OffsetAndMetadata committed = committedOffsets.get(entry.getKey());
          committedOffset = committed == null ? null : committed.offset();
        }
        result.add(new TopicPartitionOffset(entry.getKey().topic(), partition, offset,
                (committedOffset == null ? -1 : committedOffset)));
      }

      if (doCommit) {
        consumer.commitSync(committedOffsets);
      }
    } finally {
      lock.unlock();
    }
    return result;
  }


  @Override
  public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
    for (TopicPartition tp: partitions) {
      log.info("Consumer: {} Revoked: {}-{}", instanceId.toString(), tp.topic(), tp.partition());
      consumedOffsets.remove(tp);
      committedOffsets.remove(tp);
    }
  }

  @Override
  public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
    for (TopicPartition tp: partitions) {
      log.info("Consumer: {} Assigned: {}-{}", instanceId.toString(), tp.topic(), tp.partition());
    }
  }

  private class ConsumerHeartbeatThread extends Thread {
    AtomicBoolean isRunning = new AtomicBoolean(true);
    CountDownLatch shutdownLatch = new CountDownLatch(1);

    public ConsumerHeartbeatThread() {
      super("Consumer " + instanceId.getInstance() + " Heartbeat Thread");
      setDaemon(true);
    }

    @Override
    public void run() {
      while (isRunning.get()) {
        try {
          nextHeartbeatTime =
            Math.min(nextHeartbeatTime, ConsumerState.this.getNextHeartbeatTime());

          ConsumerState.this.sendHeartbeat();
          final long wait = nextHeartbeatTime - config.getTime().milliseconds();

          if (wait > 0) {
            synchronized (this) {
              wait(wait);
            }
          }
        } catch (Exception e) {
          log.warn("Heartbeat exception " + instanceId.getInstance(), e);
        }
      }
      shutdownLatch.countDown();
    }

    public void shutdown() {
      try {
        isRunning.set(false);
        this.interrupt();
        synchronized (this) {
          notify();
        }
        shutdownLatch.await();
      } catch (InterruptedException e) {
        throw new Error("Interrupted when shutting down consumer heartbeat thread.");
      }
    }
  }
}