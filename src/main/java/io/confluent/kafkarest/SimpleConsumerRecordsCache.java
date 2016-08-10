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
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;
import java.util.Queue;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;



public class SimpleConsumerRecordsCache {

  private final KafkaRestConfig config;
  private final int maxPollTime;
  private final Time time;

  private ConcurrentMap<TopicPartition, CachePerTopicPartition> highLevelCache;


  public SimpleConsumerRecordsCache(final KafkaRestConfig config) {
    this.config = config;
    this.maxPollTime = config.getInt(KafkaRestConfig.SIMPLE_CONSUMER_MAX_POLL_TIME_CONFIG);
    this.time = config.getTime();
    this.highLevelCache = new ConcurrentHashMap<>();
  }


  public List<ConsumerRecord<byte[], byte[]>> pollRecords(Consumer<byte[], byte[]> assignedConsumer,
                                                          final String topicName,
                                                          final int partitionId,
                                                          final long offset,
                                                          final long count) {
    TopicPartition topicPartition = new TopicPartition(topicName, partitionId);
    CachePerTopicPartition cache = highLevelCache.get(topicPartition);

    if (cache == null) {
      cache = new CachePerTopicPartition(config, topicPartition);
      highLevelCache.put(topicPartition, cache);
    }

    return cache.pollRecords(assignedConsumer, offset, count);
  }






  /**
   * Cache for records that were read from single topic partition.
   * Records with the higher priority replace records with lower one.
   * Higher offset means higher priority.
   * This cache improves performance if the user fetches records with
   * sequentially increasing offsets.
   */
  private static class CachePerTopicPartition {

    /**
     * Maps record offset to corresponding ConsumerRecord
     */
    private final NavigableMap<Long, ConsumerRecord<byte[], byte[]>> cachedRecords;
    private final TopicPartition topicPartition;

    private final int cacheMaxSize;
    private final int maxPollTime;
    private final Time time;
    private int cacheSize;


    public CachePerTopicPartition(final KafkaRestConfig config, final TopicPartition topicPartition) {
      this.cacheMaxSize = config.getInt(KafkaRestConfig.SIMPLE_CONSUMER_CACHE_MAX_RECORDS_CONFIG);
      this.maxPollTime = config.getInt(KafkaRestConfig.SIMPLE_CONSUMER_MAX_POLL_TIME_CONFIG);
      this.cacheSize = 0;
      this.time = config.getTime();
      this.cachedRecords = new TreeMap<>();
      this.topicPartition = topicPartition;
    }

    /**
     * Returns longest range of cached sequential records if such exists.
     * The length of returned records may be less or equal to the specified count.
     *
     * @param fromOffset offset of the first record that should be fetched
     * @param count maximum number of records that will be fetched.
     * @return list of cached sequential records if exists or null.
     */
    private List<ConsumerRecord<byte[], byte[]>> getIfExists(long fromOffset, long count) {
      SortedMap<Long, ConsumerRecord<byte[], byte[]>> cached = cachedRecords
        .subMap(fromOffset, true, fromOffset + count - 1, true);
      if (cached.size() > 0) {
        List<ConsumerRecord<byte[], byte[]>> res = new ArrayList<>();
        long currentOffset = fromOffset;
        for (ConsumerRecord<byte[], byte[]> record: cached.values()) {
          if (record.offset() == currentOffset) {
            res.add(record);
            currentOffset++;
          } else {
            break;
          }
        }
        return res;
      } else {
        return null;
      }
    }


    /**
     * Adds new record to the cache if it has greater priority
     * (greater offset) than all existing records or if the cache
     * is not empty.
     * Possible scenarios:
     * 1. If the cache is not full the record is cached
     * 2. If the cache is already full and there is a record within
     *    cache with the lower offset then this record replaces those
     *    existing. In the other case the record is not cached.
     *
     * @param record consumer record to be cached.
     */
    private boolean cacheRecord(ConsumerRecord<byte[], byte[]> record) {
      int freeSpace = cacheMaxSize - cacheSize;
      if (freeSpace > 0) {
        cachedRecords.put(record.offset(), record);
        ++cacheSize;
      } else {
        Long lowestOffset = cachedRecords.firstKey();
        if (lowestOffset < record.offset()) {
          // new record has a higher offset than the old one.
          // so cache it with replacement.
          cachedRecords.remove(lowestOffset);
          cachedRecords.put(record.offset(), record);
        } else {
          // record has a lower priority than all within cache.
          return false;
        }
      }
      return true;
    }


    private synchronized boolean doPoll(Consumer<byte[], byte[]> assignedConsumer,
                                        final List<ConsumerRecord<byte[], byte[]>> resultRecords,
                                        long startOffset,
                                        final long count,
                                        long pollTime) {

      long endTime = time.milliseconds() + pollTime;

      List<ConsumerRecord<byte[], byte[]>> appendedRecords = getIfExists(startOffset, count);

      long startPollOffset = startOffset;

      if (appendedRecords != null && !appendedRecords.isEmpty()) {
        resultRecords.addAll(appendedRecords);
        startPollOffset = appendedRecords.get(appendedRecords.size() - 1).offset() + 1;
      }

      if (resultRecords.size() < count && (pollTime = endTime - time.milliseconds()) > 0) {

        assignedConsumer.seek(topicPartition, startPollOffset);
        ConsumerRecords<byte[], byte[]> records = assignedConsumer.poll(Math.max(0, pollTime));
        if (records.isEmpty()) {
          // poll returned empty records. This means that poll was locked during remained
          // amount of time and there is no reason to continue fetching.
          // return backoff.
          return true;
        }
        Iterator<ConsumerRecord<byte[], byte[]>> it = records.iterator();

        boolean enough = false;
        while (it.hasNext()) {
          resultRecords.add(it.next());
          if (resultRecords.size() == count) {
            // fetched enough records to be returned
            enough = true;
            break;
          }
        }

        if (enough) {
          // add left records to cache
          while (it.hasNext()) {
            cacheRecord(it.next());
          }
        } else {
          if ((pollTime = endTime - time.milliseconds()) > 0) {
            // if there is time left recursively call doPoll again
            return doPoll(assignedConsumer, resultRecords,
              resultRecords.get(resultRecords.size() - 1).offset() + 1, count, pollTime);
          } else {
            // backoff
            return true;
          }

        }
      }

      return false;
    }

    public synchronized List<ConsumerRecord<byte[], byte[]>> pollRecords(Consumer<byte[], byte[]> assignedConsumer,
                                                                         final long offset,
                                                                         final long count) {
      if (count > 0) {
        List<ConsumerRecord<byte[], byte[]>> records = new ArrayList<>();
        doPoll(assignedConsumer, records, offset, count, maxPollTime);
        return records;
      }
      return null;
    }

    /**
     * If the caching is disabled this method should be used to perform
     * stateless polling without storing extra records.
     * @param assignedConsumer Consumer that is already assigned to
     *                         TopicPartition(topicName, partitionId)
     * @param topicName
     * @param partitionId
     * @param offset the offset of the next record to be fetched
     * @param count maximum number of fetched records
     * @return
     */
    public static List<ConsumerRecord<byte[], byte[]>> pollRecordsWithoutCaching(Consumer<byte[], byte[]> assignedConsumer,
                                                                                 final String topicName,
                                                                                 final int partitionId,
                                                                                 final long offset,
                                                                                 final long count,
                                                                                 long maxPollTime,
                                                                                 Time time) {

      assignedConsumer.seek(new TopicPartition(topicName, partitionId), offset);
      List<ConsumerRecord<byte[], byte[]>> result = new ArrayList<>();

      long endTime = time.milliseconds() + maxPollTime;

      Iterator<ConsumerRecord<byte[], byte[]>> it = null;
      boolean enough = false;
      while (!enough && (maxPollTime = endTime - time.milliseconds()) > 0) {

        if (it == null || !it.hasNext()) {

          ConsumerRecords<byte[], byte[]> records = assignedConsumer.poll(Math.max(0, maxPollTime));
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
  }
}
