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

import org.easymock.Capture;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import io.confluent.kafkarest.BinaryConsumerState;
import io.confluent.kafkarest.ConsumerManager;
import io.confluent.kafkarest.Errors;
import io.confluent.kafkarest.KafkaRestConfig;
import io.confluent.kafkarest.MetadataObserver;
import io.confluent.kafkarest.entities.BinaryConsumerRecord;
import io.confluent.kafkarest.entities.ConsumerInstanceConfig;
import io.confluent.kafkarest.entities.ConsumerRecord;
import io.confluent.kafkarest.entities.EmbeddedFormat;
import io.confluent.kafkarest.entities.TopicPartitionOffset;
import io.confluent.kafkarest.mock.MockConsumerConnector;
import io.confluent.kafkarest.mock.MockTime;
import io.confluent.rest.RestConfigException;
import io.confluent.rest.exceptions.RestException;
import io.confluent.rest.exceptions.RestNotFoundException;
import kafka.consumer.ConsumerConfig;
import kafka.javaapi.consumer.ConsumerConnector;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests basic create/read/commit/delete functionality of ConsumerManager. This only exercises the
 * functionality for binary data because it uses a mock consumer that only works with byte[] data.
 */
public class ConsumerManagerTest {

  private KafkaRestConfig config;
  private MetadataObserver mdObserver;
  private ConsumerManager.ConsumerFactory consumerFactory;
  private ConsumerManager consumerManager;

  private static final String groupName = "testgroup";
  private static final String topicName = "testtopic";
  private static final String secondTopicName = "testtopic2";

  private boolean sawCallback = false;

  private Capture<ConsumerConfig> capturedConsumerConfig;

  @Before
  public void setUp() throws RestConfigException {
    Properties props = new Properties();
    props.setProperty(KafkaRestConfig.CONSUMER_REQUEST_MAX_BYTES_CONFIG, "1024");
    // This setting supports the testConsumerOverrides test. It is otherwise benign and should
    // not affect other tests.
    props.setProperty("exclude.internal.topics", "false");
    config = new KafkaRestConfig(props, new MockTime());
    mdObserver = EasyMock.createMock(MetadataObserver.class);
    consumerFactory = EasyMock.createMock(ConsumerManager.ConsumerFactory.class);
    consumerManager = new ConsumerManager(config, mdObserver, consumerFactory);
  }

  private ConsumerConnector expectCreate(
      Map<String, List<Map<Integer, List<ConsumerRecord<byte[], byte[]>>>>> schedules) {
    return expectCreate(schedules, false, null);
  }

  private ConsumerConnector expectCreate(
      Map<String, List<Map<Integer, List<ConsumerRecord<byte[], byte[]>>>>> schedules,
      boolean allowMissingSchedule, String requestedId) {
    ConsumerConnector
        consumer =
        new MockConsumerConnector(
            config.getTime(), "testclient", schedules,
            Integer.parseInt(KafkaRestConfig.CONSUMER_ITERATOR_TIMEOUT_MS_DEFAULT),
            allowMissingSchedule);
    capturedConsumerConfig = new Capture<ConsumerConfig>();
    EasyMock.expect(consumerFactory.createConsumer(EasyMock.capture(capturedConsumerConfig)))
                        .andReturn(consumer);
    return consumer;
  }

  // Expect a Kafka consumer to be created, but return it with no data in its queue. Used to test
  // functionality that doesn't rely on actually consuming the data.
  private ConsumerConnector expectCreateNoData(String requestedId) {
    Map<Integer, List<ConsumerRecord<byte[], byte[]>>> referenceSchedule
        = new HashMap<Integer, List<ConsumerRecord<byte[], byte[]>>>();
    Map<String, List<Map<Integer, List<ConsumerRecord<byte[], byte[]>>>>> schedules
        = new HashMap<String, List<Map<Integer, List<ConsumerRecord<byte[], byte[]>>>>>();
    schedules.put(topicName, Arrays.asList(referenceSchedule));

    return expectCreate(schedules, true, requestedId);
  }

  private ConsumerConnector expectCreateNoData() {
    return expectCreateNoData(null);
  }

  @Test
  public void testConsumerOverrides() {
    ConsumerConnector consumer =
        new MockConsumerConnector(
            config.getTime(), "testclient", null,
            Integer.parseInt(KafkaRestConfig.CONSUMER_ITERATOR_TIMEOUT_MS_DEFAULT),
            true);
    final Capture<ConsumerConfig> consumerConfig = new Capture<ConsumerConfig>();
    EasyMock.expect(consumerFactory.createConsumer(EasyMock.capture(consumerConfig)))
        .andReturn(consumer);

    EasyMock.replay(consumerFactory);

    String cid = consumerManager.createConsumer(
        groupName, new ConsumerInstanceConfig(EmbeddedFormat.BINARY));
    // The exclude.internal.topics setting is overridden via the constructor when the
    // ConsumerManager is created, and we can make sure it gets set properly here.
    assertFalse(consumerConfig.getValue().excludeInternalTopics());

    EasyMock.verify(consumerFactory);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testConsumerNormalOps() throws InterruptedException, ExecutionException {
    // Tests create instance, read, and delete
    final List<ConsumerRecord<byte[], byte[]>> referenceRecords
        = Arrays.<ConsumerRecord<byte[], byte[]>>asList(
        new BinaryConsumerRecord("k1".getBytes(), "v1".getBytes(), 0, 0),
        new BinaryConsumerRecord("k2".getBytes(), "v2".getBytes(), 1, 0),
        new BinaryConsumerRecord("k3".getBytes(), "v3".getBytes(), 2, 0));
    Map<Integer, List<ConsumerRecord<byte[], byte[]>>>
        referenceSchedule =
        new HashMap<Integer, List<ConsumerRecord<byte[], byte[]>>>();
    referenceSchedule.put(50, referenceRecords);

    Map<String, List<Map<Integer, List<ConsumerRecord<byte[], byte[]>>>>>
        schedules =
        new HashMap<String, List<Map<Integer, List<ConsumerRecord<byte[], byte[]>>>>>();
    schedules.put(topicName, Arrays.asList(referenceSchedule));

    expectCreate(schedules);
    EasyMock.expect(mdObserver.topicExists(topicName)).andReturn(true);

    EasyMock.replay(mdObserver, consumerFactory);

    String cid = consumerManager.createConsumer(
        groupName, new ConsumerInstanceConfig(EmbeddedFormat.BINARY));
    sawCallback = false;
    consumerManager.readTopic(
        groupName, cid, topicName, BinaryConsumerState.class, Long.MAX_VALUE,
        new ConsumerManager.ReadCallback<byte[], byte[]>() {
          @Override
          public void onCompletion(List<? extends ConsumerRecord<byte[], byte[]>> records,
                                   Exception e) {
            sawCallback = true;
            assertNull(e);
            assertEquals(referenceRecords, records);
          }
        }).get();
    assertTrue(sawCallback);
    // With # of bytes in messages < max bytes per response, this should finish just after
    // the per-request timeout (because the timeout perfectly coincides with a scheduled
    // iteration when using the default settings).
    assertEquals(config.getInt(KafkaRestConfig.CONSUMER_REQUEST_TIMEOUT_MS_CONFIG)
                 + config.getInt(KafkaRestConfig.CONSUMER_ITERATOR_TIMEOUT_MS_CONFIG),
                 config.getTime().milliseconds());

    sawCallback = false;
    consumerManager.commitOffsets(groupName, cid, new ConsumerManager.CommitCallback() {
      @Override
      public void onCompletion(List<TopicPartitionOffset> offsets, Exception e) {
        sawCallback = true;
        assertNull(e);
        // Mock consumer doesn't handle offsets, so we just check we get some output for the
        // right partitions
        assertNotNull(offsets);
        assertEquals(3, offsets.size());
      }
    }).get();
    assertTrue(sawCallback);

    consumerManager.deleteConsumer(groupName, cid);

    EasyMock.verify(mdObserver, consumerFactory);
  }

  @Test
  public void testConsumerMaxBytesResponse() throws InterruptedException, ExecutionException {
    // Tests that when there are more records available than the max bytes to be included in the
    // response, not all of it is returned.
    final List<ConsumerRecord<byte[], byte[]>> referenceRecords
        = Arrays.<ConsumerRecord<byte[], byte[]>>asList(
        new BinaryConsumerRecord(null, new byte[512], 0, 0),
        new BinaryConsumerRecord(null, new byte[512], 1, 0),
        new BinaryConsumerRecord(null, new byte[512], 2, 0),
        new BinaryConsumerRecord(null, new byte[512], 3, 0)
    );
    Map<Integer, List<ConsumerRecord<byte[], byte[]>>> referenceSchedule
        = new HashMap<Integer, List<ConsumerRecord<byte[], byte[]>>>();
    referenceSchedule.put(50, referenceRecords);

    Map<String, List<Map<Integer, List<ConsumerRecord<byte[], byte[]>>>>> schedules
        = new HashMap<String, List<Map<Integer, List<ConsumerRecord<byte[], byte[]>>>>>();
    schedules.put(topicName, Arrays.asList(referenceSchedule));

    expectCreate(schedules);
    EasyMock.expect(mdObserver.topicExists(topicName)).andReturn(true);
    EasyMock.expect(mdObserver.topicExists(topicName)).andReturn(true);

    EasyMock.replay(mdObserver, consumerFactory);

    String cid = consumerManager.createConsumer(
        groupName, new ConsumerInstanceConfig(EmbeddedFormat.BINARY));
    sawCallback = false;
    consumerManager.readTopic(
        groupName, cid, topicName, BinaryConsumerState.class, Long.MAX_VALUE,
        new ConsumerManager.ReadCallback<byte[], byte[]>() {
          @Override
          public void onCompletion(List<? extends ConsumerRecord<byte[], byte[]>> records,
                                   Exception e) {
            sawCallback = true;
            assertNull(e);
            // Should only see the first two messages since the third pushes us over the limit.
            assertEquals(2, records.size());
          }
        }).get();
    assertTrue(sawCallback);

    // Also check the user-submitted limit
    sawCallback = false;
    consumerManager.readTopic(
        groupName, cid, topicName, BinaryConsumerState.class, 512,
        new ConsumerManager.ReadCallback<byte[], byte[]>() {
          @Override
          public void onCompletion(List<? extends ConsumerRecord<byte[], byte[]>> records,
                                   Exception e) {
            sawCallback = true;
            assertNull(e);
            // Should only see first message since the user specified an even smaller size limit
            assertEquals(1, records.size());
          }
        }).get();
    assertTrue(sawCallback);

    consumerManager.deleteConsumer(groupName, cid);

    EasyMock.verify(mdObserver, consumerFactory);
  }

  @Test
  public void testIDOverridesName() {
    // We should remain compatible with the original use of consumer IDs, even if it shouldn't
    // really be used. Specifying any ID should override any naming to ensure the same behavior
    expectCreateNoData("id");
    EasyMock.replay(mdObserver, consumerFactory);

    String cid = consumerManager.createConsumer(
        groupName,
        new ConsumerInstanceConfig("id", "name", EmbeddedFormat.BINARY.toString(), null, null)
    );
    assertEquals("id", cid);
    assertEquals("id", capturedConsumerConfig.getValue().consumerId().getOrElse(null));
    EasyMock.verify(mdObserver, consumerFactory);
  }

  @Test
  public void testDuplicateConsumerName() {
    expectCreateNoData();
    EasyMock.replay(mdObserver, consumerFactory);

    consumerManager.createConsumer(
        groupName,
        new ConsumerInstanceConfig(null, "name", EmbeddedFormat.BINARY.toString(), null, null)
    );

    try {
      consumerManager.createConsumer(
          groupName,
          new ConsumerInstanceConfig(null, "name", EmbeddedFormat.BINARY.toString(), null, null)
      );
      fail("Expected to see exception because consumer already exists");
    } catch (RestException e) {
      // expected
      assertEquals(Errors.CONSUMER_ALREADY_EXISTS_ERROR_CODE, e.getErrorCode());
    }

    EasyMock.verify(mdObserver, consumerFactory);
  }

  @Test
  public void testMultipleTopicSubscriptionsFail() throws InterruptedException, ExecutionException {
    expectCreateNoData();
    EasyMock.expect(mdObserver.topicExists(topicName)).andReturn(true);
    EasyMock.expect(mdObserver.topicExists(secondTopicName)).andReturn(true);
    EasyMock.replay(mdObserver, consumerFactory);

    String cid = consumerManager.createConsumer(groupName,
                                                new ConsumerInstanceConfig(EmbeddedFormat.BINARY));
    sawCallback = false;
    consumerManager.readTopic(
        groupName, cid, topicName, BinaryConsumerState.class, Long.MAX_VALUE,
        new ConsumerManager.ReadCallback<byte[], byte[]>() {
          @Override
          public void onCompletion(List<? extends ConsumerRecord<byte[], byte[]>> records,
                                   Exception e) {
            sawCallback = true;
            assertNull(e);
            assertEquals(0, records.size());
          }
        }).get();
    assertTrue(sawCallback);

    // Attempt to read from second topic should result in an exception
    sawCallback = false;
    consumerManager.readTopic(
        groupName, cid, secondTopicName, BinaryConsumerState.class, Long.MAX_VALUE,
        new ConsumerManager.ReadCallback<byte[], byte[]>() {
          @Override
          public void onCompletion(List<? extends ConsumerRecord<byte[], byte[]>> records,
                                   Exception e) {
            sawCallback = true;
            assertNotNull(e);
            assertTrue(e instanceof RestException);
            assertEquals(Errors.CONSUMER_ALREADY_SUBSCRIBED_ERROR_CODE,
                         ((RestException) e).getErrorCode());
            assertNull(records);
          }
        }).get();
    assertTrue(sawCallback);

    consumerManager.deleteConsumer(groupName, cid);

    EasyMock.verify(mdObserver, consumerFactory);
  }

  @Test
  public void testReadInvalidInstanceFails() {
    readAndExpectImmediateNotFound("invalid", topicName);
  }

  @Test
  public void testReadInvalidTopicFails() throws InterruptedException, ExecutionException {
    final String invalidTopicName = "invalidtopic";
    expectCreate(null);
    EasyMock.expect(mdObserver.topicExists(invalidTopicName)).andReturn(false);

    EasyMock.replay(mdObserver, consumerFactory);

    String instanceId = consumerManager.createConsumer(
        groupName, new ConsumerInstanceConfig(EmbeddedFormat.BINARY));
    readAndExpectImmediateNotFound(instanceId, invalidTopicName);

    EasyMock.verify(mdObserver, consumerFactory);
  }

  @Test(expected = RestNotFoundException.class)
  public void testDeleteInvalidConsumer() {
    consumerManager.deleteConsumer(groupName, "invalidinstance");
  }


  @Test
  public void testConsumerExceptions() throws InterruptedException, ExecutionException {
    // We should be able to handle an exception thrown by the consumer, then issue another
    // request that succeeds and still see all the data
    final List<ConsumerRecord<byte[], byte[]>> referenceRecords
        = Arrays.<ConsumerRecord<byte[], byte[]>>asList(
        new BinaryConsumerRecord("k1".getBytes(), "v1".getBytes(), 0, 0),
        null, // trigger consumer exception
        new BinaryConsumerRecord("k2".getBytes(), "v2".getBytes(), 1, 0),
        new BinaryConsumerRecord("k3".getBytes(), "v3".getBytes(), 2, 0));
    Map<Integer, List<ConsumerRecord<byte[], byte[]>>>
        referenceSchedule =
        new HashMap<Integer, List<ConsumerRecord<byte[], byte[]>>>();
    referenceSchedule.put(50, referenceRecords);

    Map<String, List<Map<Integer, List<ConsumerRecord<byte[], byte[]>>>>>
        schedules =
        new HashMap<String, List<Map<Integer, List<ConsumerRecord<byte[], byte[]>>>>>();
    schedules.put(topicName, Arrays.asList(referenceSchedule));

    expectCreate(schedules);
    EasyMock.expect(mdObserver.topicExists(topicName)).andReturn(true).times(2);

    EasyMock.replay(mdObserver, consumerFactory);

    String cid = consumerManager.createConsumer(
        groupName, new ConsumerInstanceConfig(EmbeddedFormat.BINARY));

    // First read should result in exception.
    sawCallback = false;
    consumerManager.readTopic(
        groupName, cid, topicName, BinaryConsumerState.class, Long.MAX_VALUE,
        new ConsumerManager.ReadCallback<byte[], byte[]>() {
          @Override
          public void onCompletion(List<? extends ConsumerRecord<byte[], byte[]>> records,
                                   Exception e) {
            sawCallback = true;
            assertNull(records);
            assertNotNull(e);
          }
        }).get();
    assertTrue(sawCallback);

    // Second read should recover and return all the data.
    sawCallback = false;
    consumerManager.readTopic(
        groupName, cid, topicName, BinaryConsumerState.class, Long.MAX_VALUE,
        new ConsumerManager.ReadCallback<byte[], byte[]>() {
          @Override
          public void onCompletion(List<? extends ConsumerRecord<byte[], byte[]>> records,
                                   Exception e) {
            sawCallback = true;
            assertNull(e);
            assertEquals(referenceRecords, records);
          }
        }).get();
    assertTrue(sawCallback);

    EasyMock.verify(mdObserver, consumerFactory);
  }

  private void readAndExpectImmediateNotFound(String cid, String topic) {
    sawCallback = false;
    Future
        future =
        consumerManager.readTopic(
            groupName, cid, topic, BinaryConsumerState.class, Long.MAX_VALUE,
            new ConsumerManager.ReadCallback<byte[], byte[]>() {
              @Override
              public void onCompletion(List<? extends ConsumerRecord<byte[], byte[]>> records,
                                       Exception e) {
                sawCallback = true;
                assertNull(records);
                assertThat(e, instanceOf(RestNotFoundException.class));
              }
            });
    assertTrue(sawCallback);
    assertNull(future);
  }
}
