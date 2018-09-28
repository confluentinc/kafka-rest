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

import io.confluent.kafkarest.ConsumerManager;
import io.confluent.kafkarest.ConsumerReadCallback;
import io.confluent.kafkarest.Errors;
import io.confluent.kafkarest.KafkaRestConfig;
import io.confluent.kafkarest.MetadataObserver;
import io.confluent.kafkarest.BinaryConsumerState;

import io.confluent.kafkarest.SystemTime;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import io.confluent.kafkarest.entities.BinaryConsumerRecord;
import io.confluent.kafkarest.entities.ConsumerInstanceConfig;
import io.confluent.kafkarest.entities.ConsumerRecord;
import io.confluent.kafkarest.entities.EmbeddedFormat;
import io.confluent.kafkarest.entities.TopicPartitionOffset;
import io.confluent.kafkarest.mock.MockConsumerConnector;
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

  // Setup holding vars for results from callback
  private boolean sawCallback = false;
  private static Exception actualException = null;
  private static List<? extends ConsumerRecord<byte[], byte[]>> actualRecords = null;
  private int actualLength = 0;
  private static List<TopicPartitionOffset> actualOffsets = null;

  private Capture<ConsumerConfig> capturedConsumerConfig;

  @Before
  public void setUp() throws RestConfigException {
    Properties props = new Properties();
    props.setProperty(KafkaRestConfig.CONSUMER_REQUEST_MAX_BYTES_CONFIG, "1024");
    // This setting supports the testConsumerOverrides test. It is otherwise benign and should
    // not affect other tests.
    props.setProperty("exclude.internal.topics", "false");
    config = new KafkaRestConfig(props, new SystemTime());
    mdObserver = EasyMock.createMock(MetadataObserver.class);
    consumerFactory = EasyMock.createMock(ConsumerManager.ConsumerFactory.class);
    consumerManager = new ConsumerManager(config, mdObserver, consumerFactory);
  }

  @After
  public void tearDown() {
    consumerManager.shutdown();
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
  public void testConsumerNormalOps() throws Exception {
    // Tests create instance, read, and delete
    final List<ConsumerRecord<byte[], byte[]>> referenceRecords
        = Arrays.<ConsumerRecord<byte[], byte[]>>asList(
        new BinaryConsumerRecord(topicName, "k1".getBytes(), "v1".getBytes(), 0, 0),
        new BinaryConsumerRecord(topicName, "k2".getBytes(), "v2".getBytes(), 1, 0),
        new BinaryConsumerRecord(topicName, "k3".getBytes(), "v3".getBytes(), 2, 0));
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
    actualException = null;
    actualRecords = null;
    readTopic(cid);

    assertTrue("Callback failed to fire", sawCallback);
    assertNull("No exception in callback", actualException);
    assertEquals("Records returned not as expected", referenceRecords, actualRecords);

    sawCallback = false;
    actualException = null;
    actualOffsets = null;
    consumerManager.commitOffsets(groupName, cid, new ConsumerManager.CommitCallback() {
      @Override
      public void onCompletion(List<TopicPartitionOffset> offsets, Exception e) {
        sawCallback = true;

        actualException = e;
        actualOffsets = offsets;
      }
    }).get();
    assertTrue("Callback not called", sawCallback);
    assertNull("Callback exception", actualException);
    // Mock consumer doesn't handle offsets, so we just check we get some output for the
    // right partitions
    assertNotNull("Callback Offsets", actualOffsets);
    assertEquals("Callback Offsets Size", 3, actualOffsets.size());

    consumerManager.deleteConsumer(groupName, cid);

    EasyMock.verify(mdObserver, consumerFactory);
  }

  @Test
  public void testConsumerMaxBytesResponse() throws Exception {
    // Tests that when there are more records available than the max bytes to be included in the
    // response, not all of it is returned.
    final List<ConsumerRecord<byte[], byte[]>> referenceRecords
        = Arrays.<ConsumerRecord<byte[], byte[]>>asList(
        // Don't use 512 as this happens to fall on boundary
        new BinaryConsumerRecord(topicName, null, new byte[511], 0, 0),
        new BinaryConsumerRecord(topicName, null, new byte[511], 1, 0),
        new BinaryConsumerRecord(topicName, null, new byte[511], 2, 0),
        new BinaryConsumerRecord(topicName, null, new byte[511], 3, 0)
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
    // Ensure vars used by callback are correctly initialised.
    sawCallback = false;
    actualException = null;
    actualLength = 0;
    readTopic(cid, new ConsumerReadCallback<byte[], byte[]>() {
      @Override
      public void onCompletion(List<? extends ConsumerRecord<byte[], byte[]>> records,
                               RestException e) {
        sawCallback = true;
        actualException = e;
        // Should only see the first two messages since the third pushes us over the limit.
        actualLength = records.size();
      }
    });
    assertTrue("Callback failed to fire", sawCallback);
    assertNull("Callback received exception", actualException);
    // Should only see the first two messages since the third pushes us over the limit.
    assertEquals("List of records returned incorrect", 2, actualLength);

    // Also check the user-submitted limit
    sawCallback = false;
    actualException = null;
    actualLength = 0;
    readTopic(cid, 512, new ConsumerReadCallback<byte[], byte[]>() {
      @Override
      public void onCompletion(List<? extends ConsumerRecord<byte[], byte[]>> records,
                               RestException e) {
        sawCallback = true;
        actualException = e;
        // Should only see the first two messages since the third pushes us over the limit.
        actualLength = records.size();
      }
    });
    assertTrue("Callback failed to fire", sawCallback);
    assertNull("Callback received exception", actualException);
    // Should only see the first two messages since the third pushes us over the limit.
    assertEquals("List of records returned incorrect", 1, actualLength);

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
  public void testMultipleTopicSubscriptionsFail() throws Exception {
    expectCreateNoData();
    EasyMock.expect(mdObserver.topicExists(topicName)).andReturn(true);
    EasyMock.expect(mdObserver.topicExists(secondTopicName)).andReturn(true);
    EasyMock.replay(mdObserver, consumerFactory);

    String cid = consumerManager.createConsumer(groupName,
                                                new ConsumerInstanceConfig(EmbeddedFormat.BINARY));
    sawCallback = false;
    actualException = null;
    actualRecords = null;
    readTopic(cid);
    assertTrue("Callback not called", sawCallback);
    assertNull("Callback exception", actualException);
    assertEquals("Callback records should be valid but of 0 size", 0, actualRecords.size());


    // Attempt to read from second topic should result in an exception
    sawCallback = false;
    actualException = null;
    actualRecords = null;
    readTopic(cid, secondTopicName);
    assertTrue("Callback failed to fire", sawCallback);
    assertNotNull("Callback failed to receive an exception", actualException);
    assertTrue("Callback Exception should be an instance of RestException", actualException instanceof RestException);
    assertEquals("Callback Exception should be for already subscribed consumer", Errors.CONSUMER_ALREADY_SUBSCRIBED_ERROR_CODE,
                 ((RestException) actualException).getErrorCode());
    assertNull("Given an exception occurred in callback shouldn't be any records returned", actualRecords);

    consumerManager.deleteConsumer(groupName, cid);

    EasyMock.verify(mdObserver, consumerFactory);
  }

  @Test
  public void testReadInvalidInstanceFails() {
    readAndExpectImmediateNotFound("invalid", topicName);
  }

  @Test
  public void testReadInvalidTopicFails() throws Exception, ExecutionException {
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
  public void testConsumerExceptions() throws Exception {
    // We should be able to handle an exception thrown by the consumer, then issue another
    // request that succeeds and still see all the data
    final List<ConsumerRecord<byte[], byte[]>> referenceRecords
        = Arrays.<ConsumerRecord<byte[], byte[]>>asList(
        new BinaryConsumerRecord(topicName, "k1".getBytes(), "v1".getBytes(), 0, 0),
        null, // trigger consumer exception
        new BinaryConsumerRecord(topicName, "k2".getBytes(), "v2".getBytes(), 1, 0),
        new BinaryConsumerRecord(topicName, "k3".getBytes(), "v3".getBytes(), 2, 0));
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
    actualException = null;
    actualRecords = null;
    readTopic(cid);
    assertTrue("Callback not called", sawCallback);
    assertNotNull("Callback exception should be populated", actualException);
    assertNull("Callback with exception should not have any records", actualRecords);

    // Second read should recover and return all the data.
    sawCallback = false;
    readTopic(cid, new ConsumerReadCallback<byte[], byte[]>() {
      @Override
      public void onCompletion(List<? extends ConsumerRecord<byte[], byte[]>> records,
                               RestException e) {
        sawCallback = true;
        assertNull(e);
        assertEquals(referenceRecords, records);
      }
    });
    assertTrue(sawCallback);

    EasyMock.verify(mdObserver, consumerFactory);
  }

  /**
   * We need to wait for topic reads to finish. We can't rely on the returned Future
   * because one whole read task is spanned throughout multiple Runnables.
   * They are created and scheduled on the executor until the whole task is finished
   */
  private void readTopic(final String cid) throws Exception {
    readTopic(cid, topicName);
  }

  private void readTopic(final String cid, String topic) throws Exception {
    readTopic(cid, topic, Long.MAX_VALUE, new ConsumerReadCallback<byte[], byte[]>() {
      @Override
      public void onCompletion(List<? extends ConsumerRecord<byte[], byte[]>> records,
                               RestException e) {
        sawCallback = true;
        actualRecords = records;
        actualException = e;
      }
    });
  }

  private void readTopic(final String cid, final ConsumerReadCallback callback) throws Exception {
    readTopic(cid, topicName, Long.MAX_VALUE, callback);
  }

  private void readTopic(final String cid, long maxBytes, final ConsumerReadCallback callback) throws Exception {
    readTopic(cid, topicName, maxBytes, callback);
  }

  private void readTopic(final String cid, String topic, long maxBytes, final ConsumerReadCallback callback) throws Exception {
    // Add a bit of fuzz to the wait time
    long maxTimeout = (long) (Integer.parseInt(KafkaRestConfig.CONSUMER_REQUEST_TIMEOUT_MS_DEFAULT) * 1.10);
    Future future = consumerManager.readTopic(groupName, cid, topic, BinaryConsumerState.class, maxBytes, callback);
    future.get(maxTimeout, TimeUnit.MILLISECONDS);
  }

  private void readAndExpectImmediateNotFound(String cid, String topic) {
    sawCallback = false;
    actualRecords = null;
    actualException = null;
    Future
        future =
        consumerManager.readTopic(
            groupName, cid, topic, BinaryConsumerState.class, Long.MAX_VALUE,
            new ConsumerReadCallback<byte[], byte[]>() {
              @Override
              public void onCompletion(List<? extends ConsumerRecord<byte[], byte[]>> records,
                                       RestException e) {
                sawCallback = true;
                actualRecords = records;
                actualException = e;
              }
            });
    assertTrue("Callback not called", sawCallback);
    assertNull("Callback records", actualRecords);
    assertThat("Callback exception is RestNotFound", actualException, instanceOf(RestNotFoundException.class));
    assertNull(future);
  }
}
