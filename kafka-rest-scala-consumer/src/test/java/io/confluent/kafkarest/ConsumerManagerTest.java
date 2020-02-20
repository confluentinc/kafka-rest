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
package io.confluent.kafkarest;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import io.confluent.kafkarest.entities.ConsumerInstanceConfig;
import io.confluent.kafkarest.entities.ConsumerRecord;
import io.confluent.kafkarest.entities.EmbeddedFormat;
import io.confluent.kafkarest.entities.TopicPartitionOffset;
import io.confluent.kafkarest.mock.MockConsumerConnector;
import io.confluent.rest.RestConfigException;
import io.confluent.rest.exceptions.RestException;
import io.confluent.rest.exceptions.RestNotFoundException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import kafka.consumer.ConsumerConfig;
import kafka.javaapi.consumer.ConsumerConnector;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests basic create/read/commit/delete functionality of ConsumerManager. This only exercises the
 * functionality for binary data because it uses a mock consumer that only works with byte[] data.
 */
public class ConsumerManagerTest {

  private Properties properties;
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
  private static List<ConsumerRecord<byte[], byte[]>> actualRecords = null;
  private int actualLength = 0;
  private static List<TopicPartitionOffset> actualOffsets = null;

  private Capture<ConsumerConfig> capturedConsumerConfig;

  @Before
  public void setUp() throws RestConfigException {
    this.properties = new Properties();
    properties.setProperty(KafkaRestConfig.CONSUMER_REQUEST_MAX_BYTES_CONFIG, "1024");
    // This setting supports the testConsumerOverrides test. It is otherwise benign and should
    // not affect other tests.
    properties.setProperty("exclude.internal.topics", "false");
    setUp(properties);
  }

  public void setUp(Properties properties) throws RestConfigException {
    config = new KafkaRestConfig(properties, new SystemTime());
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
    capturedConsumerConfig = Capture.newInstance();
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
    final Capture<ConsumerConfig> consumerConfig = Capture.newInstance();
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
    final List<ConsumerRecord<byte[], byte[]>> referenceRecords = referenceRecords(3);
    Map<Integer, List<ConsumerRecord<byte[], byte[]>>>
        referenceSchedule =
            new HashMap<>();
    referenceSchedule.put(50, referenceRecords);

    Map<String, List<Map<Integer, List<ConsumerRecord<byte[], byte[]>>>>>
        schedules =
        new HashMap<String, List<Map<Integer, List<ConsumerRecord<byte[], byte[]>>>>>();
    schedules.put(topicName, Arrays.asList(referenceSchedule));

    expectCreate(schedules);
    EasyMock.expect(mdObserver.topicExists(topicName)).andReturn(true);

    EasyMock.replay(mdObserver, consumerFactory);
    String cid = consumerManager.createConsumer(groupName, new ConsumerInstanceConfig(EmbeddedFormat.BINARY));
    readFromDefault(cid);

    verifyRead(referenceRecords, null);

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

  /**
   * Response should return no sooner than KafkaRestConfig.CONSUMER_REQUEST_TIMEOUT_MS_CONFIG
   */
  @Test
  public void testConsumerRequestTimeoutMs() throws Exception {
    Integer expectedRequestTimeoutMs = 400;
    properties.setProperty(KafkaRestConfig.CONSUMER_REQUEST_TIMEOUT_MS_CONFIG, expectedRequestTimeoutMs.toString());
    setUp(properties);
    Map<String, List<Map<Integer, List<ConsumerRecord<byte[], byte[]>>>>>
            schedules =
            new HashMap<>();
    Map<Integer, List<ConsumerRecord<byte[], byte[]>>> referenceSchedule = new HashMap<>();
    schedules.put(topicName, Arrays.asList(referenceSchedule));
    expectCreate(schedules);

    EasyMock.expect(mdObserver.topicExists(topicName)).andReturn(true);
    EasyMock.replay(mdObserver, consumerFactory);

    long startTime = System.currentTimeMillis();
    readFromDefault(consumerManager.createConsumer(groupName, new ConsumerInstanceConfig(EmbeddedFormat.BINARY)));
    assertTrue(System.currentTimeMillis() - startTime > expectedRequestTimeoutMs);
    assertTrue("Callback failed to fire", sawCallback);
    assertNull("No exception in callback", actualException);
  }

  /**
   * When min.bytes is fulfilled, we should return immediately
   */
  @Test
  public void testConsumerTimeoutMsMsAndMinBytes() throws Exception {
    properties.setProperty(KafkaRestConfig.CONSUMER_REQUEST_TIMEOUT_MS_CONFIG, "1303");
    properties.setProperty(KafkaRestConfig.PROXY_FETCH_MIN_BYTES_CONFIG, "1");
    setUp(properties);

    final List<ConsumerRecord<byte[], byte[]>> referenceRecords = referenceRecords(3);
    Map<Integer, List<ConsumerRecord<byte[], byte[]>>>
            referenceSchedule = new HashMap<>();
    referenceSchedule.put(50, referenceRecords);

    Map<String, List<Map<Integer, List<ConsumerRecord<byte[], byte[]>>>>>
            schedules = new HashMap<>();
    schedules.put(topicName, Arrays.asList(referenceSchedule));

    expectCreate(schedules);
    EasyMock.expect(mdObserver.topicExists(topicName)).andReturn(true);
    EasyMock.replay(mdObserver, consumerFactory);

    String cid = consumerManager.createConsumer(groupName, new ConsumerInstanceConfig(EmbeddedFormat.BINARY));
    long startTime = System.currentTimeMillis();
    readFromDefault(cid);

    assertTrue("Callback failed to fire", sawCallback);
    assertNull("No exception in callback", actualException);
    // should return first record immediately since min.bytes is fulfilled
    assertEquals("Records returned not as expected",
            Arrays.asList(referenceRecords.get(0)), actualRecords);
    long estimatedTime = System.currentTimeMillis() - startTime;
    int timeoutMs = config.getInt(KafkaRestConfig.CONSUMER_REQUEST_TIMEOUT_MS_CONFIG);
    assertTrue(estimatedTime < timeoutMs); // should have returned earlier than consumer.request.timeout.ms
  }

  @Test
  public void testConsumeMinBytesIsOverridablePerConsumer() throws Exception {
    properties.setProperty(KafkaRestConfig.PROXY_FETCH_MIN_BYTES_CONFIG, "10");
    // global settings should return more than one record immediately
    setUp(properties);

    final List<ConsumerRecord<byte[], byte[]>> referenceRecords = referenceRecords(3);
    Map<Integer, List<ConsumerRecord<byte[], byte[]>>>
            referenceSchedule = new HashMap<>();
    referenceSchedule.put(50, referenceRecords);

    Map<String, List<Map<Integer, List<ConsumerRecord<byte[], byte[]>>>>>
            schedules = new HashMap<>();
    schedules.put(topicName, Arrays.asList(referenceSchedule));

    expectCreate(schedules);
    EasyMock.expect(mdObserver.topicExists(topicName)).andReturn(true);
    EasyMock.replay(mdObserver, consumerFactory);

    // we expect one record to be returned since the setting is overridden
    ConsumerInstanceConfig config =
        new ConsumerInstanceConfig(
            /* id= */ null,
            /* name= */ null,
            EmbeddedFormat.BINARY,
            /* autoOffsetReset= */ null,
            /* autoCommitEnable= */ null,
            /* responseMinBytes= */ 1,
            /* requestWaitMs= */ null);
    readFromDefault(consumerManager.createConsumer(groupName, config));

    assertTrue("Callback failed to fire", sawCallback);
    assertNull("No exception in callback", actualException);
    // should return first record immediately since min.bytes is fulfilled
    assertEquals("Records returned not as expected",
            Arrays.asList(referenceRecords.get(0)), actualRecords);
  }

  /**
   * Response should return no sooner than the overridden CONSUMER_REQUEST_TIMEOUT_MS_CONFIG
   */
  @Test
  public void testConsumerRequestTimeoutMsIsOverriddablePerConsumer() throws Exception {
    Integer overriddenRequestTimeMs = 111;
    Integer globalRequestTimeMs = 1201;
    properties.setProperty(KafkaRestConfig.CONSUMER_REQUEST_TIMEOUT_MS_CONFIG, globalRequestTimeMs.toString());
    setUp(properties);
    Map<String, List<Map<Integer, List<ConsumerRecord<byte[], byte[]>>>>>
            schedules =
            new HashMap<>();
    Map<Integer, List<ConsumerRecord<byte[], byte[]>>> referenceSchedule = new HashMap<>();
    schedules.put(topicName, Arrays.asList(referenceSchedule));
    expectCreate(schedules);

    EasyMock.expect(mdObserver.topicExists(topicName)).andReturn(true);
    EasyMock.replay(mdObserver, consumerFactory);

    ConsumerInstanceConfig consumerConfig =
        new ConsumerInstanceConfig(
            /* id= */ null,
            /* name= */ null,
            EmbeddedFormat.BINARY,
            /* autoOffsetReset= */ null,
            /* autoCommitEnable= */ null,
            /* responseMinBytes= */ null,
            overriddenRequestTimeMs);
    String cid = consumerManager.createConsumer(groupName, consumerConfig);
    long startTime = System.currentTimeMillis();
    readFromDefault(cid);
    long elapsedTime = System.currentTimeMillis() - startTime;
    assertTrue(elapsedTime < globalRequestTimeMs);
    assertTrue(elapsedTime > overriddenRequestTimeMs);

    assertTrue("Callback failed to fire", sawCallback);
    assertNull("No exception in callback", actualException);
  }

  @Test
  public void testConsumerMaxBytesResponse() throws Exception {
    // Tests that when there are more records available than the max bytes to be included in the
    // response, not all of it is returned.
    final List<ConsumerRecord<byte[], byte[]>> referenceRecords
        = Arrays.<ConsumerRecord<byte[], byte[]>>asList(
        // Don't use 512 as this happens to fall on boundary
        new ConsumerRecord<>(topicName, null, new byte[511], 0, 0),
        new ConsumerRecord<>(topicName, null, new byte[511], 1, 0),
        new ConsumerRecord<>(topicName, null, new byte[511], 2, 0),
        new ConsumerRecord<>(topicName, null, new byte[511], 3, 0)
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
      public void onCompletion(List<ConsumerRecord<byte[], byte[]>> records, Exception e) {
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
      public void onCompletion(List<ConsumerRecord<byte[], byte[]>> records, Exception e) {
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
        new ConsumerInstanceConfig(
            /* id= */ "id",
            /* name= */ "name",
            EmbeddedFormat.BINARY,
            /* autoOffsetReset= */ null,
            /* autoCommitEnable= */ null,
            /* responseMinBytes= */ null,
            /* requestWaitMs= */ null));
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
        new ConsumerInstanceConfig(
            /* id= */ null,
            /* name= */ "name",
            EmbeddedFormat.BINARY,
            /* autoOffsetReset= */ null,
            /* autoCommitEnable= */ null,
            /* responseMinBytes= */ null,
            /* requestWaitMs= */ null));

    try {
      consumerManager.createConsumer(
          groupName,
          new ConsumerInstanceConfig(
              /* id= */ null,
              /* name= */ "name",
              EmbeddedFormat.BINARY,
              /* autoOffsetReset= */ null,
              /* autoCommitEnable= */ null,
              /* responseMinBytes= */ null,
              /* requestWaitMs= */ null));
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
    verifyRead(Collections.<ConsumerRecord<byte[],byte[]>>emptyList(), null);
    assertTrue("Callback not called", sawCallback);
    assertNull("Callback exception", actualException);
    assertEquals("Callback records should be valid but of 0 size", 0, actualRecords.size());


    // Attempt to read from second topic should result in an exception
    sawCallback = false;
    actualException = null;
    actualRecords = null;
    readTopic(cid, secondTopicName);
    verifyRead(null, RestException.class);
    assertEquals("Callback Exception should be for already subscribed consumer", Errors.CONSUMER_ALREADY_SUBSCRIBED_ERROR_CODE,
                 ((RestException) actualException).getErrorCode());

    consumerManager.deleteConsumer(groupName, cid);

    EasyMock.verify(mdObserver, consumerFactory);
  }


  @Test
  public void testBackoffMsControlsPollCalls() throws Exception {
    Properties props = new Properties();
    props.setProperty(KafkaRestConfig.CONSUMER_ITERATOR_BACKOFF_MS_CONFIG, "250");
    // This setting supports the testConsumerOverrides test. It is otherwise benign and should
    // not affect other tests.
    props.setProperty("exclude.internal.topics", "false");
    config = new KafkaRestConfig(props, new SystemTime());
    mdObserver = EasyMock.createMock(MetadataObserver.class);
    consumerFactory = EasyMock.createMock(ConsumerManager.ConsumerFactory.class);
    consumerManager = new ConsumerManager(config, mdObserver, consumerFactory);
    expectCreateNoData();
    EasyMock.expect(mdObserver.topicExists(topicName)).andReturn(true);
    EasyMock.replay(mdObserver, consumerFactory);
    String cid = consumerManager.createConsumer(groupName,
            new ConsumerInstanceConfig(EmbeddedFormat.BINARY));
    Future f = readTopicFuture(cid, topicName, Long.MAX_VALUE, new ConsumerReadCallback<byte[], byte[]>() {
      @Override
      public void onCompletion(List<ConsumerRecord<byte[], byte[]>> records, Exception e) {
        sawCallback = true;
        actualException = e;
      }
    });

    // backoff is 250
    Thread.sleep(100);
    // backoff should be in place right now. the read task should be delayed and re-ran until the max.bytes or timeout is hit
    assertEquals(1, consumerManager.delayedReadTasks.size());
    Thread.sleep(100);
    assertEquals(1, consumerManager.delayedReadTasks.size());
    f.get();
    assertTrue("Callback not called", sawCallback);
    assertNull("Callback exception should not be populated", actualException);
  }

  @Test
  public void testBackoffMsUpdatesReadTaskExpiry() throws Exception {
    Properties props = new Properties();
    int backoffMs = 1000;
    props.setProperty(KafkaRestConfig.CONSUMER_ITERATOR_BACKOFF_MS_CONFIG, Integer.toString(backoffMs));
    // This setting supports the testConsumerOverrides test. It is otherwise benign and should
    // not affect other tests.
    props.setProperty("exclude.internal.topics", "false");
    config = new KafkaRestConfig(props, new SystemTime());
    mdObserver = EasyMock.createMock(MetadataObserver.class);
    consumerFactory = EasyMock.createMock(ConsumerManager.ConsumerFactory.class);
    consumerManager = new ConsumerManager(config, mdObserver, consumerFactory);
    expectCreateNoData();
    EasyMock.expect(mdObserver.topicExists(topicName)).andReturn(true);
    EasyMock.replay(mdObserver, consumerFactory);
    String cid = consumerManager.createConsumer(groupName,
            new ConsumerInstanceConfig(EmbeddedFormat.BINARY));
    Future f = readTopicFuture(cid, topicName, Long.MAX_VALUE, new ConsumerReadCallback<byte[], byte[]>() {
      @Override
      public void onCompletion(List<ConsumerRecord<byte[], byte[]>> records, Exception e) {
        sawCallback = true;
        actualRecords = records;
        actualException = e;
      }
    });

    Thread.sleep(100);
    ConsumerManager.RunnableReadTask readTask = consumerManager.delayedReadTasks.peek();
    if (readTask == null) {
      fail("Could not get read task in time. It should not be null");
    }
    long delay = readTask.getDelay(TimeUnit.MILLISECONDS);
    assertTrue(delay < backoffMs);
    assertTrue(delay > (backoffMs * 0.5));
    f.get();
    verifyRead(Collections.<ConsumerRecord<byte[],byte[]>>emptyList(), null);
  }

  @Test
  public void testConsumerExpirationIsUpdated() throws Exception {
    expectCreateNoData();
    EasyMock.expect(mdObserver.topicExists(topicName)).andReturn(true);
    EasyMock.replay(mdObserver, consumerFactory);
    String cid = consumerManager.createConsumer(
            groupName, new ConsumerInstanceConfig(EmbeddedFormat.BINARY));
    ConsumerState state = consumerManager.getConsumerInstance(groupName, cid);
    long initialExpiration = state.expiration;

    Future f = readTopicFuture(cid, topicName, Long.MAX_VALUE, new ConsumerReadCallback<byte[], byte[]>() {
      @Override
      public void onCompletion(List<ConsumerRecord<byte[], byte[]>> records, Exception e) {
        sawCallback = true;
        actualRecords = records;
        actualException = e;
      }
    });
    Thread.sleep(100);
    assertTrue(state.expiration > initialExpiration);
    initialExpiration = state.expiration;

    f.get();
    assertTrue(state.expiration > initialExpiration);

    verifyRead(Collections.<ConsumerRecord<byte[],byte[]>>emptyList(), null);
    initialExpiration = state.expiration;
    consumerManager.commitOffsets(groupName, cid, new ConsumerManager.CommitCallback() {
      @Override
      public void onCompletion(List<TopicPartitionOffset> offsets, Exception e) {
        sawCallback = true;

        actualException = e;
        actualOffsets = offsets;
      }
    }).get();
    assertTrue(state.expiration > initialExpiration);
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
    final List<ConsumerRecord<byte[], byte[]>> referenceRecords = referenceRecords(3);
    referenceRecords.add(null); // trigger exception
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
      public void onCompletion(List<ConsumerRecord<byte[], byte[]>> records, Exception e) {
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
      public void onCompletion(List<ConsumerRecord<byte[], byte[]>> records, Exception e) {
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
    Future future = readTopicFuture(cid, topic, maxBytes, callback);
    future.get(maxTimeout, TimeUnit.MILLISECONDS);
  }

  private Future readTopicFuture(String cid, String topic, long maxBytes, final ConsumerReadCallback callback) {
    return consumerManager.readTopic(groupName, cid, topic, BinaryConsumerState.class, maxBytes, callback);
  }

  /**
   * Returns a list of one record per partition, up to {@code count} partitions
   */
  private List<ConsumerRecord<byte[], byte[]>> referenceRecords(int count) {
    List<ConsumerRecord<byte[], byte[]>> records = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      records.add(
          new ConsumerRecord<>(
              topicName,
              ("k" + (i + 1)).getBytes(),
              ("v" + (i + 1)).getBytes(), i, 0)
      );
    }
    return records;
  }

  private void readFromDefault(String cid) throws InterruptedException, ExecutionException {
    sawCallback = false;
    actualException = null;
    actualRecords = null;
    consumerManager.readTopic(
            groupName, cid, topicName, BinaryConsumerState.class, Long.MAX_VALUE,
        new ConsumerReadCallback<byte[], byte[]>() {
          @Override
          public void onCompletion(List<ConsumerRecord<byte[], byte[]>> records, Exception e) {
            actualException = e;
            actualRecords = records;
            sawCallback = true;
          }
        }).get();
  }

  private void readAndExpectImmediateNotFound(String cid, String topic) {
    sawCallback = false;
    actualRecords = null;
    actualException = null;
    Future future = readTopicFuture(cid, topic, Long.MAX_VALUE, new ConsumerReadCallback<byte[], byte[]>() {
      @Override
      public void onCompletion(List<ConsumerRecord<byte[], byte[]>> records, Exception e) {
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

  private void verifyRead(List<ConsumerRecord<byte[], byte[]>> records, Class exception) {
    assertTrue("Callback was not called", sawCallback);
    if (records == null) {
      assertNull("Callback records should be null", actualRecords);
    } else {
      assertEquals("Callback records not as expected", records, actualRecords);
    }

    if (exception == null) {
      assertNull("Exception is not null", actualException);
    } else {
      assertNotNull(actualException);
      assertThat("Callback exception is not as expected", actualException, instanceOf(exception));
    }
  }
}
