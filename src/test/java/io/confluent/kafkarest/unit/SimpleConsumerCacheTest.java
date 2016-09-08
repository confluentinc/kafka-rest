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
package io.confluent.kafkarest.unit;

import io.confluent.kafkarest.KafkaRestConfig;
import io.confluent.kafkarest.SimpleConsumerRecordsCache;
import io.confluent.kafkarest.Time;
import io.confluent.kafkarest.entities.AbstractConsumerRecord;
import io.confluent.kafkarest.entities.BinaryConsumerRecord;
import io.confluent.kafkarest.mock.MockKafkaConsumer;
import io.confluent.kafkarest.mock.MockTime;
import io.confluent.rest.RestConfigException;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

public class SimpleConsumerCacheTest {

  private static final int MAX_CACHED_RECORDS = 50;

  private KafkaRestConfig config;
  private Time time;
  private Consumer<byte[], byte[]> mockConsumer;
  private SimpleConsumerRecordsCache cache;

  @Before
  public void setUp() throws RestConfigException {
    time = new MockTime();
    Properties properties = new Properties();
    properties.put(KafkaRestConfig.SIMPLE_CONSUMER_CACHE_MAX_RECORDS_CONFIG, MAX_CACHED_RECORDS);
    properties.put(KafkaRestConfig.SIMPLE_CONSUMER_MAX_POLL_TIME_CONFIG, 1000);
    properties.put(KafkaRestConfig.SIMPLE_CONSUMER_MAX_CACHES_NUM_CONFIG, 3);
    config = new KafkaRestConfig(properties, time);
    cache = new SimpleConsumerRecordsCache(config);
    mockConsumer = EasyMock.createMock(MockKafkaConsumer.class);
  }

  /**
   * Tests cached record size and their offsets.
   * Extra polled records with higher offsets should
   * replace cached records with lowest offsets within cache
   * if there is not space left to store new records.
   */
  @Test
  public void testCachedRecords() throws RestConfigException {
    final String topic = "topic";
    final int partition = 0;
    final int count = 50;
    final int delay = 100;
    List<List<AbstractConsumerRecord<byte[], byte[]>>> scheduledRecords;
    List<Long> scheduledTimePoints;
    List<org.apache.kafka.clients.consumer.ConsumerRecord<byte[], byte[]>> result;

    // initialize records to be returned by mock consumer
    final int startOffset1 = 200;
    final int rec1Size = 100;
    final List<AbstractConsumerRecord<byte[], byte[]>> referenceRecords1 = new ArrayList<>();
    initializeRecords(referenceRecords1, topic, partition, startOffset1, rec1Size);
    scheduledRecords = new ArrayList<>();
    scheduledTimePoints = new ArrayList<>();
    scheduledRecords.add(referenceRecords1);
    scheduledTimePoints.add((long) delay);
    Consumer<byte[], byte[]> consumer1 = new MockKafkaConsumer(scheduledRecords, scheduledTimePoints, time);
    consumer1.assign(Collections.singletonList(new TopicPartition(topic, partition)));

    final int startOffset2 = startOffset1 + rec1Size;
    final int rec2Size = 50;
    final List<AbstractConsumerRecord<byte[], byte[]>> referenceRecords2 = new ArrayList<>();
    initializeRecords(referenceRecords2, topic, partition, startOffset2, rec2Size);
    scheduledRecords = new ArrayList<>();
    scheduledTimePoints = new ArrayList<>();
    scheduledRecords.add(referenceRecords2);
    scheduledTimePoints.add((long) delay);
    Consumer<byte[], byte[]> consumer2 = new MockKafkaConsumer(scheduledRecords, scheduledTimePoints, time);
    consumer2.assign(Collections.singletonList(new TopicPartition(topic, partition)));


    result = cache.pollRecords(consumer1, topic, partition, startOffset1, count);
    // records size should not exceed count
    Assert.assertEquals(result.size(), count);
    checkRecordsIdentity(referenceRecords1.subList(0, count), result);
    Assert.assertEquals(time.milliseconds(), delay);
    // cache contains records with offsets [250...300)

    // check that all unprocessed records are taken cache.
    EasyMock.expect(mockConsumer.poll(EasyMock.anyLong()))
      .andThrow(new AssertionError("There should be enough records within cache"));
    mockConsumer.seek(EasyMock.eq(new TopicPartition(topic, partition)), EasyMock.eq(startOffset1 + count));
    EasyMock.expectLastCall();
    EasyMock.replay(mockConsumer);
    result = cache.pollRecords(mockConsumer, topic, partition, startOffset1 + count, count);
    checkRecordsIdentity(referenceRecords1.subList(count, count * 2), result);

    // offsets [280...300) are returned from cache and 1 record with offset 300
    // is polled by consumer. There are extra offsets [301...350) fetched.
    // The final state of cache should be [299] U [301...350)
    // poll should not be invoked.
    result = cache.pollRecords(consumer2, topic, partition, 280, 21);

    List<AbstractConsumerRecord<byte[], byte[]>> expected = referenceRecords1.subList(80, 100);
    expected.add(referenceRecords2.get(0));
    checkRecordsIdentity(expected, result);

    // check that records with higher offsets overwrites records with lower ones.
    mockConsumer = EasyMock.createMock(MockKafkaConsumer.class);
    EasyMock.expect(mockConsumer.poll(EasyMock.anyLong()))
      .andThrow(new AssertionError("There should be enough records within cache"));
    mockConsumer.seek(EasyMock.eq(new TopicPartition(topic, partition)), EasyMock.eq(301));
    EasyMock.expectLastCall();
    EasyMock.replay(mockConsumer);
    result = cache.pollRecords(mockConsumer, topic, partition, 301, 49);
    checkRecordsIdentity(referenceRecords2.subList(1, 50), result);

  }


  private void initializeRecords(List<AbstractConsumerRecord<byte[], byte[]>> records,
                                 String topic,
                                 int partition,
                                 long startOffset,
                                 int size) {

    for (int i = 0; i < size; i++) {
      records.add(new BinaryConsumerRecord(
        ("k" + i).getBytes(), ("v" + i).getBytes(), topic, partition, startOffset + i));
    }
  }

  private void checkRecordsIdentity(List<AbstractConsumerRecord<byte[], byte[]>> expected,
                                    List<org.apache.kafka.clients.consumer.ConsumerRecord<byte[], byte[]>> actual) {
    Assert.assertEquals(expected.size(), actual.size());
    for (int i = 0; i < expected.size(); i++) {
      org.apache.kafka.clients.consumer.ConsumerRecord<byte[], byte[]> actualRecord = actual.get(i);
      AbstractConsumerRecord<byte[], byte[]> expectedRecord = expected.get(i);
      Assert.assertEquals(actualRecord.offset(), expectedRecord.getOffset());
      Assert.assertEquals(actualRecord.topic(), expectedRecord.getTopic());
      Assert.assertEquals(actualRecord.partition(), expectedRecord.getPartition());
      Assert.assertEquals(actualRecord.value(), expectedRecord.getValue());
    }
  }

}
