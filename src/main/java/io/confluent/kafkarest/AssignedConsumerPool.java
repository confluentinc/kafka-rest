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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The AssignedConsumerPool keeps a pool of SimpleConsumers
 * and can increase the pool within a specified limit
 */
public class AssignedConsumerPool {
  private static final Logger log = LoggerFactory.getLogger(AssignedConsumerPool.class);

  // maxPoolSize = 0 means unlimited
  private final int maxPoolSize;
  // poolInstanceAvailabilityTimeoutMs = 0 means there is no timeout
  private final int poolInstanceAvailabilityTimeoutMs;
  private final Time time;

  private final int maxPollTime;

  private final ConsumerFactory<byte[], byte[]> simpleConsumerFactory;
  private final Map<TopicPartition, Map<Long, List<Consumer<byte[], byte[]>>>> topicPartitionOffsetConsumers;
  private int consumerSize;

  public AssignedConsumerPool(int maxPoolSize,
                            int poolInstanceAvailabilityTimeoutMs,
                            int maxPollTime,
                            Time time, ConsumerFactory<byte[], byte[]> simpleConsumerFactory) {
    this.maxPoolSize = maxPoolSize;
    this.poolInstanceAvailabilityTimeoutMs = poolInstanceAvailabilityTimeoutMs;
    this.time = time;
    this.maxPollTime = maxPollTime;

    this.simpleConsumerFactory = simpleConsumerFactory;

    // using LinkedHashMap guarantees FIFO iteration order
    topicPartitionOffsetConsumers = new LinkedHashMap<>();
    consumerSize = 0;
  }

  public RecordsFetcher getRecordsFetcher(TopicPartition topicPartition) {
    return new RecordsFetcher(topicPartition);
  }

  public final class RecordsFetcher implements AutoCloseable {

    private Consumer<byte[], byte[]> consumer;
    private final TopicPartition topicPartition;

    RecordsFetcher(TopicPartition topicPartition) {
      this.topicPartition = topicPartition;
    }

    // support for lazy initialization gives opportunity
    // to add some kind of cache in future.
    private Consumer<byte[], byte[]> initializeConsumer(long offset) {
      if (consumer == null) {
        consumer = AssignedConsumerPool.this.get(topicPartition, offset);
      }
      return consumer;
    }

    public List<ConsumerRecord<byte[], byte[]>> poll(final long offset, final long count) {
      if (count == 0) {
        return Collections.emptyList();
      }

      List<ConsumerRecord<byte[], byte[]>> result = new ArrayList<>();

      long endTime = time.milliseconds() + maxPollTime;
      long pollTime;

      Iterator<ConsumerRecord<byte[], byte[]>> it = null;
      boolean enough = false;

      while (!enough &&
          (pollTime = endTime - time.milliseconds()) > 0) {

        if (it == null || !it.hasNext()) {

          if (consumer == null) {
            initializeConsumer(offset);
          }

          ConsumerRecords<byte[], byte[]> records = consumer
              .poll(Math.max(0, pollTime));
          it = records.iterator();
        }

        while (it.hasNext()) {
          ConsumerRecord<byte[], byte[]> record = it.next();
          result.add(record);
          if (result.size() == count) {
            enough = true;
            break;
          }
        }
      }
      return result;
    }

    public void close() throws Exception {
      if (consumer != null) {
        AssignedConsumerPool.this.release
            (consumer, topicPartition, consumer.position(topicPartition));
      }
    }
  }

  // Get consumer that stays the longest time in the topicPartitionConsumers for a given topicPartition
  // The method should be invoked from synchronized context
  private Consumer<byte[], byte[]> longestWaitingConsumer(TopicPartition topicPartition, Map<Long, List<Consumer<byte[], byte[]>>> topicPartitionConsumers) {
    Iterator<Map.Entry<Long, List<Consumer<byte[], byte[]>>>> iterator =
        topicPartitionConsumers.entrySet().iterator();
    Map.Entry<Long, List<Consumer<byte[], byte[]>>> offsetsEntry = iterator.next();

    Consumer<byte[], byte[]> consumer = offsetsEntry.getValue().remove(0);
    if (offsetsEntry.getValue().isEmpty()) {
      iterator.remove();
      if (topicPartitionOffsetConsumers.get(topicPartition).isEmpty()) {
        topicPartitionOffsetConsumers.remove(topicPartition);
      }
    }
    return consumer;
  }

  /**
   * @return assigned Consumer that is ready to be used for polling records from startOffset
   */
  synchronized public Consumer<byte[], byte[]> get(TopicPartition topicPartition, long startOffset) {

    final long expiration = time.milliseconds() + poolInstanceAvailabilityTimeoutMs;

    while (true) {

      // If there is a KafkaConsumer available
      if (!topicPartitionOffsetConsumers.isEmpty()) {
        Map<Long, List<Consumer<byte[], byte[]>>> consumersMap =
          topicPartitionOffsetConsumers.get(topicPartition);

        Consumer<byte[], byte[]> consumer;

        if (consumersMap != null) {
          // consumers with the same offset
          List<Consumer<byte[], byte[]>> consumers = consumersMap.remove(startOffset);
          if (consumers != null) {

            consumer = consumers.remove(0);
            if (!consumers.isEmpty()) {
              consumersMap.put(startOffset, consumers);
            }

          } else {
            // there is no consumer with needed offset present.
            // get the longest waiting consumer that is assigned for requested topic partition.
            consumer = longestWaitingConsumer(topicPartition, consumersMap);

            consumer.seek(topicPartition, startOffset);
          }
        } else {
          // there is no consumer assigned for requested topic partition present.
          // get the longest waiting consumer.
          Iterator<Map.Entry<TopicPartition, Map<Long, List<Consumer<byte[], byte[]>>>>> iterator =
            topicPartitionOffsetConsumers.entrySet().iterator();
          Map.Entry<TopicPartition, Map<Long, List<Consumer<byte[], byte[]>>>> topicPartitionsEntry =
            iterator.next();

          consumer = longestWaitingConsumer(topicPartition, topicPartitionsEntry.getValue());

          consumer.unsubscribe();
          consumer.assign(Collections.singletonList(topicPartition));
          consumer.seek(topicPartition, startOffset);
        }

        return consumer;
      }

      // If not consumer is available, but we can instantiate a new one
      if (consumerSize < maxPoolSize || maxPoolSize == 0) {

        Consumer<byte[], byte[]> consumer = simpleConsumerFactory.createConsumer();
        consumer.assign(Collections.singletonList(topicPartition));
        consumer.seek(topicPartition, startOffset);
        ++consumerSize;
        log.debug("Create new KafkaConsumer. Pool size {}", consumerSize);
        return consumer;
      }

      // If no consumer is available and we reached the limit
      try {
        // The behavior of wait when poolInstanceAvailabilityTimeoutMs=0 is consistent as it won't timeout
        wait(poolInstanceAvailabilityTimeoutMs);
      } catch (InterruptedException e) {
        log.warn("A thread requesting a SimpleConsumer has been interrupted while waiting", e);
      }

      // In some cases ("spurious wakeup", see wait() doc), the thread will resume before the timeout
      // We have to guard against that and throw only if the timeout has expired for real
      if (time.milliseconds() > expiration && poolInstanceAvailabilityTimeoutMs != 0) {
        throw Errors.simpleConsumerPoolTimeoutException();
      }
    }
  }

  synchronized public void release(Consumer<byte[], byte[]> consumer, TopicPartition topicPartition, long offset) {
    Map<Long, List<Consumer<byte[], byte[]>>> offsetsMap = topicPartitionOffsetConsumers.get(topicPartition);
    if (offsetsMap == null) {
      offsetsMap = new LinkedHashMap<>();
      topicPartitionOffsetConsumers.put(topicPartition, offsetsMap);
    }

    List<Consumer<byte[], byte[]>> consumers = offsetsMap.get(offset);
    if (consumers == null) {
      consumers = new LinkedList<>();
      offsetsMap.put(offset, consumers);
    }

    consumers.add(consumer);

    notify();
  }

  public synchronized void shutdown() {
    log.debug("Shutting down SimpleConsumer pool");
    for (Map<Long, List<Consumer<byte[], byte[]>>> offsetMap: topicPartitionOffsetConsumers.values()) {
      for (List<Consumer<byte[], byte[]>> consumers: offsetMap.values()) {
        for (Consumer<byte[], byte[]> consumer: consumers) {
          consumer.wakeup();
          consumer.close();
        }
      }
    }
  }

  public int size() {
    return consumerSize;
  }
}