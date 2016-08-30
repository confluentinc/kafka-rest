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

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * The SimpleConsumerPool keeps a pool of SimpleConsumers
 * and can increase the pool within a specified limit
 */
public class SimpleConsumerPool {
  private static final Logger log = LoggerFactory.getLogger(SimpleConsumerPool.class);

  // maxPoolSize = 0 means unlimited
  private final int maxPoolSize;
  // poolInstanceAvailabilityTimeoutMs = 0 means there is no timeout
  private final int poolInstanceAvailabilityTimeoutMs;
  private final Time time;

  private final int maxPollTime;

  private final ConsumerFactory<byte[], byte[]> simpleConsumerFactory;
  private final Map<TopicPartition, Map<Long, List<Consumer<byte[], byte[]>>>> topicPartitionOffsetConsumers;
  private int consumerSize;

  public SimpleConsumerPool(int maxPoolSize,
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
    private final SimpleConsumerPool ownerPool;
    private final long maxPollTime;
    private boolean initialized;
    private Time time;
    private final TopicPartition topicPartition;

    RecordsFetcher(TopicPartition topicPartition) {
      this.ownerPool = SimpleConsumerPool.this;
      this.time = SimpleConsumerPool.this.time;
      this.maxPollTime = SimpleConsumerPool.this.maxPollTime;
      this.topicPartition = topicPartition;
      initialized = false;
    }

    // support for lazy initialization gives opportunity
    // to add some kind of cache in future.
    private Consumer<byte[], byte[]> initializeConsumer(long offset) {
      if (!initialized) {
        consumer = ownerPool.get(topicPartition, offset);
        initialized = true;
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

          if (!initialized) {
            initializeConsumer(offset);
          }

          ConsumerRecords<byte[], byte[]> records = consumer
              .poll(Math.max(0, pollTime));
          if (records.isEmpty()) {
            continue;
          }
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
      if (initialized) {
        ownerPool.release
            (consumer, topicPartition, consumer.position(topicPartition));
      }
    }
  }


  /**
   * @return assigned Consumer that is ready to be used for polling records
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
            Iterator<Map.Entry<Long, List<Consumer<byte[], byte[]>>>> iterator =
              consumersMap.entrySet().iterator();
            Map.Entry<Long, List<Consumer<byte[], byte[]>>> offsetsEntry =
              iterator.next();

            consumer = offsetsEntry.getValue().remove(0);
            if (offsetsEntry.getValue().isEmpty()) {
              iterator.remove();
              if (topicPartitionOffsetConsumers.get(topicPartition).isEmpty()) {
                topicPartitionOffsetConsumers.remove(topicPartition);
              }
            }

            consumer.seek(topicPartition, startOffset);
          }
        } else {
          // there is no consumer assigned for requested topic partition present.
          // get the longest waiting consumer.
          Iterator<Map.Entry<TopicPartition, Map<Long, List<Consumer<byte[], byte[]>>>>> iterator =
            topicPartitionOffsetConsumers.entrySet().iterator();
          Map.Entry<TopicPartition, Map<Long, List<Consumer<byte[], byte[]>>>> topicPartitionsEntry =
            iterator.next();

          Iterator<Map.Entry<Long, List<Consumer<byte[], byte[]>>>> iterator1 =
            topicPartitionsEntry.getValue().entrySet().iterator();
          Map.Entry<Long, List<Consumer<byte[], byte[]>>> offsetsEntry =
            iterator1.next();

          consumer = offsetsEntry.getValue().remove(0);
          if (offsetsEntry.getValue().isEmpty()) {
            iterator1.remove();
            if (topicPartitionsEntry.getValue().isEmpty()) {
              iterator.remove();
            }
          }

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
    Map<Long, List<Consumer<byte[], byte[]>>> offsetsMap =
      topicPartitionOffsetConsumers.get(topicPartition);
    if (offsetsMap != null) {
      List<Consumer<byte[], byte[]>> consumers = offsetsMap.get(offset);
      if (consumers != null) {
        consumers.add(consumer);
      } else {
        consumers = new LinkedList<>();
        consumers.add(consumer);
        offsetsMap.put(offset, consumers);
      }
    } else {
      List<Consumer<byte[], byte[]>> consumers = new LinkedList<>();
      consumers.add(consumer);
      offsetsMap = new HashMap<>();
      offsetsMap.put(offset, consumers);
      topicPartitionOffsetConsumers.put(topicPartition, offsetsMap);
    }
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