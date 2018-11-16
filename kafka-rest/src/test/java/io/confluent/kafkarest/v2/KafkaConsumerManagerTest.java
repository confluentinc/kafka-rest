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
package io.confluent.kafkarest.v2;

import io.confluent.kafkarest.SystemTime;
import io.confluent.kafkarest.ConsumerReadCallback;
import io.confluent.kafkarest.entities.ConsumerOffsetCommitRequest;
import io.confluent.kafkarest.entities.ConsumerRecord;
import io.confluent.kafkarest.entities.ConsumerSubscriptionRecord;
import io.confluent.rest.exceptions.RestException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.easymock.EasyMockRunner;
import org.easymock.Mock;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import io.confluent.kafkarest.KafkaRestConfig;
import io.confluent.kafkarest.MetadataObserver;
import io.confluent.kafkarest.entities.BinaryConsumerRecord;
import io.confluent.kafkarest.entities.ConsumerInstanceConfig;
import io.confluent.kafkarest.entities.EmbeddedFormat;
import io.confluent.kafkarest.entities.TopicPartitionOffset;
import io.confluent.rest.RestConfigException;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

/**
 * Tests basic create/read/commit/delete functionality of ConsumerManager. This only exercises the
 * functionality for binary data because it uses a mock consumer that only works with byte[] data.
 */
@RunWith(EasyMockRunner.class)
public class KafkaConsumerManagerTest {

    private KafkaRestConfig config;
    @Mock
    private MetadataObserver mdObserver;
    @Mock
    private KafkaConsumerManager.KafkaConsumerFactory consumerFactory;
    private KafkaConsumerManager consumerManager;

    private static final String groupName = "testgroup";
    private static final String topicName = "testtopic";

    // Setup holding vars for results from callback
    private boolean sawCallback = false;
    private static Exception actualException = null;
    private static List<? extends ConsumerRecord<byte[], byte[]>> actualRecords = null;
    private static List<TopicPartitionOffset> actualOffsets = null;

    private Capture<Properties> capturedConsumerConfig;

    private MockConsumer<byte[], byte[]> consumer;


    @Before
    public void setUp() throws RestConfigException {
        setUpConsumer(setUpProperties());
    }

    private void setUpConsumer(Properties properties) throws RestConfigException {
        config = new KafkaRestConfig(properties, new SystemTime());
        consumerManager = new KafkaConsumerManager(config, consumerFactory);
        consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST, groupName);
    }

    private Properties setUpProperties() {
        return setUpProperties(null);
    }

    private Properties setUpProperties(Properties props) {
        if (props == null) {
            props = new Properties();
        }
        props.setProperty(KafkaRestConfig.BOOTSTRAP_SERVERS_CONFIG, "PLAINTEXT://hostname:9092");
        props.setProperty(KafkaRestConfig.CONSUMER_REQUEST_MAX_BYTES_CONFIG, "1024");
        props.setProperty(KafkaRestConfig.CONSUMER_MAX_THREADS_CONFIG, "1");
        // This setting supports the testConsumerOverrides test. It is otherwise benign and should
        // not affect other tests.
        props.setProperty("consumer." + ConsumerConfig.EXCLUDE_INTERNAL_TOPICS_CONFIG, "false");

        return props;
    }

    @After
    public void tearDown() {
        consumerManager.shutdown();
    }

    private void expectCreate(MockConsumer consumer) {
        capturedConsumerConfig = Capture.newInstance();
        Properties props = EasyMock.capture(capturedConsumerConfig);
        EasyMock.expect(consumerFactory.createConsumer(props))
                .andStubReturn(consumer);
        EasyMock.replay(consumerFactory);
    }

    @Test
    public void testConsumerOverrides() {
        final Capture<Properties> consumerConfig = Capture.newInstance();
        EasyMock.expect(consumerFactory.createConsumer(EasyMock.capture(consumerConfig)))
                .andReturn(consumer);

        EasyMock.replay(consumerFactory);

        consumerManager.createConsumer(groupName, new ConsumerInstanceConfig(EmbeddedFormat.BINARY));
        // The exclude.internal.topics setting is overridden via the constructor when the
        // ConsumerManager is created, and we can make sure it gets set properly here.
        assertEquals("false", consumerConfig.getValue().get(ConsumerConfig.EXCLUDE_INTERNAL_TOPICS_CONFIG));

        EasyMock.verify(consumerFactory);
    }

    /**
     * Response should return no sooner than KafkaRestConfig.CONSUMER_REQUEST_TIMEOUT_MS_CONFIG
     */
    @Test
    public void testConsumerRequestTimeoutms() throws Exception {
        Properties props = setUpProperties(new Properties());
        props.setProperty(KafkaRestConfig.CONSUMER_REQUEST_TIMEOUT_MS_CONFIG, "2500");
        setUpConsumer(props);

        expectCreate(consumer);
        String cid = consumerManager.createConsumer(
            groupName, new ConsumerInstanceConfig(EmbeddedFormat.BINARY));
        consumerManager.subscribe(groupName, cid, new ConsumerSubscriptionRecord(Collections.singletonList(topicName), null));

        readFromDefault(cid);
        Thread.sleep((long) (config.getInt(KafkaRestConfig.CONSUMER_REQUEST_TIMEOUT_MS_CONFIG) * 0.5));
        assertFalse("Callback failed early", sawCallback);
        Thread.sleep((long) (config.getInt(KafkaRestConfig.CONSUMER_REQUEST_TIMEOUT_MS_CONFIG) * 0.7));
        assertTrue("Callback failed to fire", sawCallback);
        assertNull("No exception in callback", actualException);
    }


    /**
     * Response should return no sooner than KafkaRestConfig.PROXY_FETCH_MAX_WAIT_MS_CONFIG
     */
    @Test
    public void testConsumerWaitMs() throws Exception {
        Properties props = setUpProperties(new Properties());
        Integer expectedRequestTimeoutms = 400;
        props.setProperty(KafkaRestConfig.CONSUMER_REQUEST_TIMEOUT_MS_CONFIG, expectedRequestTimeoutms.toString());
        setUpConsumer(props);

        expectCreate(consumer);
        schedulePoll();

        String cid = consumerManager.createConsumer(
                groupName, new ConsumerInstanceConfig(EmbeddedFormat.BINARY));
        consumerManager.subscribe(groupName, cid, new ConsumerSubscriptionRecord(Collections.singletonList(topicName), null));
        consumer.rebalance(Collections.singletonList(new TopicPartition(topicName, 0)));
        consumer.updateBeginningOffsets(Collections.singletonMap(new TopicPartition(topicName, 0), 0L));

        readFromDefault(cid);
        long startTime = System.currentTimeMillis();
        while ((System.currentTimeMillis() - startTime) < expectedRequestTimeoutms) {
            assertFalse(sawCallback);
            Thread.sleep(40);
        }
        Thread.sleep((long) (expectedRequestTimeoutms * 0.5));
        assertTrue("Callback failed to fire", sawCallback);
        assertNull("No exception in callback", actualException);
    }

    /**
     * When min.bytes is not fulfilled, we should return after consumer.request.timeout.ms
     * When min.bytes is fulfilled, we should return immediately
     */
    @Test
    public void testConsumerRequestTimeoutmsAndMinBytes() throws Exception {
        Properties props = setUpProperties(new Properties());
        props.setProperty(KafkaRestConfig.CONSUMER_REQUEST_TIMEOUT_MS_CONFIG, "1303");
        props.setProperty(KafkaRestConfig.PROXY_FETCH_MIN_BYTES_CONFIG, "5");
        setUpConsumer(props);

        expectCreate(consumer);

        String cid = consumerManager.createConsumer(
                groupName, new ConsumerInstanceConfig(EmbeddedFormat.BINARY));
        consumerManager.subscribe(groupName, cid, new ConsumerSubscriptionRecord(Collections.singletonList(topicName), null));
        consumer.rebalance(Collections.singletonList(new TopicPartition(topicName, 0)));
        consumer.updateBeginningOffsets(Collections.singletonMap(new TopicPartition(topicName, 0), 0L));

        long startTime = System.currentTimeMillis();
        readFromDefault(cid);
        int expectedRequestTimeoutms = config.getInt(KafkaRestConfig.CONSUMER_REQUEST_TIMEOUT_MS_CONFIG);
        while ((System.currentTimeMillis() - startTime) < expectedRequestTimeoutms) {
            assertFalse(sawCallback);
            Thread.sleep(100);
        }
        Thread.sleep(200);
        assertTrue("Callback failed to fire", sawCallback);
        assertNull("No exception in callback", actualException);
        assertTrue("Records returned not empty", actualRecords.isEmpty());


        sawCallback = false;
        final List<ConsumerRecord<byte[], byte[]>> referenceRecords = schedulePoll();
        readFromDefault(cid);
        Thread.sleep(expectedRequestTimeoutms / 2); // should return in less time

        assertEquals("Records returned not as expected", referenceRecords, actualRecords);
        assertTrue("Callback not called", sawCallback);
        assertNull("Callback exception", actualException);
    }

    /**
     * Should return more than min bytes of records and less than max bytes.
     * Should not poll() twice after min bytes have been reached
     */
    @Test
    public void testConsumerMinAndMaxBytes() throws Exception {
        BinaryConsumerRecord sampleRecord = binaryConsumerRecord(0);
        int sampleRecordSize = sampleRecord.getKey().length + sampleRecord.getValue().length;
        // we expect all the records from the first poll to be returned
        Properties props = setUpProperties(new Properties());
        props.setProperty(KafkaRestConfig.PROXY_FETCH_MIN_BYTES_CONFIG, Integer.toString(sampleRecordSize));
        props.setProperty(KafkaRestConfig.CONSUMER_REQUEST_MAX_BYTES_CONFIG, Integer.toString(sampleRecordSize * 10));
        setUpConsumer(props);

        final List<ConsumerRecord<byte[], byte[]>> scheduledRecords = schedulePoll();
        final List<ConsumerRecord<byte[], byte[]>> referenceRecords = Arrays.asList(scheduledRecords.get(0), scheduledRecords.get(1), scheduledRecords.get(2));
        schedulePoll(3);

        expectCreate(consumer);
        String cid = consumerManager.createConsumer(
                groupName, new ConsumerInstanceConfig(EmbeddedFormat.BINARY));
        consumerManager.subscribe(groupName, cid, new ConsumerSubscriptionRecord(Collections.singletonList(topicName), null));
        consumer.rebalance(Collections.singletonList(new TopicPartition(topicName, 0)));
        consumer.updateBeginningOffsets(Collections.singletonMap(new TopicPartition(topicName, 0), 0L));

        readFromDefault(cid);
        Thread.sleep((long) (Integer.parseInt(KafkaRestConfig.CONSUMER_REQUEST_TIMEOUT_MS_DEFAULT) * 0.5)); // should return sooner since min bytes hit

        assertTrue("Callback failed to fire", sawCallback);
        assertNull("No exception in callback", actualException);
        assertEquals("Records returned not as expected", referenceRecords, actualRecords);
    }

    @Test
    public void testConsumeMinBytesIsOverridablePerConsumer() throws Exception {
        BinaryConsumerRecord sampleRecord = binaryConsumerRecord(0);
        int sampleRecordSize = sampleRecord.getKey().length + sampleRecord.getValue().length;
        Properties props = setUpProperties(new Properties());
        props.setProperty(KafkaRestConfig.PROXY_FETCH_MIN_BYTES_CONFIG, Integer.toString(sampleRecordSize * 5));
        props.setProperty(KafkaRestConfig.CONSUMER_REQUEST_MAX_BYTES_CONFIG, Integer.toString(sampleRecordSize * 6));
        setUpConsumer(props);

        final List<ConsumerRecord<byte[], byte[]>> scheduledRecords = schedulePoll();
        // global settings would make the consumer call poll twice and get more than 3 records,
        // overridden settings should make him poll once since the min bytes will be reached
        final List<ConsumerRecord<byte[], byte[]>> referenceRecords = Arrays.asList(scheduledRecords.get(0),
                scheduledRecords.get(1),
                scheduledRecords.get(2));
        schedulePoll(3);

        expectCreate(consumer);
        ConsumerInstanceConfig config = new ConsumerInstanceConfig(EmbeddedFormat.BINARY);
        // we expect three records to be returned since the setting is overridden and poll() wont be called a second time
        config.setResponseMinBytes(sampleRecordSize * 2);
        String cid = consumerManager.createConsumer(
                groupName, config);
        consumerManager.subscribe(groupName, cid, new ConsumerSubscriptionRecord(Collections.singletonList(topicName), null));
        consumer.rebalance(Collections.singletonList(new TopicPartition(topicName, 0)));
        consumer.updateBeginningOffsets(Collections.singletonMap(new TopicPartition(topicName, 0), 0L));

        readFromDefault(cid);
        Thread.sleep((long) (Integer.parseInt(KafkaRestConfig.CONSUMER_REQUEST_TIMEOUT_MS_DEFAULT) * 0.5)); // should return sooner since min bytes hit

        assertTrue("Callback failed to fire", sawCallback);
        assertNull("No exception in callback", actualException);
        assertEquals("Records returned not as expected", referenceRecords, actualRecords);
    }

    @Test
    public void testConsumerNormalOps() throws InterruptedException, ExecutionException {
        final List<ConsumerRecord<byte[], byte[]>> referenceRecords = bootstrapConsumer(consumer);

        sawCallback = false;
        actualException = null;
        actualRecords = null;
        readFromDefault(consumer.cid());
        Thread.sleep((long) (Integer.parseInt(KafkaRestConfig.CONSUMER_REQUEST_TIMEOUT_MS_DEFAULT) * 1.10));

        assertTrue("Callback failed to fire", sawCallback);
        assertNull("No exception in callback", actualException);
        assertEquals("Records returned not as expected", referenceRecords, actualRecords);
        sawCallback = false;
        actualException = null;
        actualOffsets = null;
        ConsumerOffsetCommitRequest commitRequest = null; // Commit all offsets
        consumerManager.commitOffsets(groupName, consumer.cid(), null, commitRequest, new KafkaConsumerManager.CommitCallback() {
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
        // TODO: Currently the values are not actually returned in the callback nor in the response.
        //assertEquals("Callback Offsets Size", 3, actualOffsets.size());

        consumerManager.deleteConsumer(groupName, consumer.cid());
    }

    @Test
    public void testBackoffMsControlsPollCalls() throws Exception {
        bootstrapConsumer(consumer);
        consumerManager.readRecords(groupName, consumer.cid(), BinaryKafkaConsumerState.class, -1, Long.MAX_VALUE,
                new ConsumerReadCallback<byte[], byte[]>() {
                    @Override
                    public void onCompletion(List<? extends ConsumerRecord<byte[], byte[]>> records, RestException e) {
                        actualException = e;
                        actualRecords = records;
                        sawCallback = true;
                    }
                });

        // backoff is 250
        Thread.sleep(100);
        // backoff should be in place right now. the read task should be delayed and re-ran until the max.bytes or timeout is hit
        assertEquals(1, consumerManager.delayedReadTasks.size());
        Thread.sleep(100);
        assertEquals(1, consumerManager.delayedReadTasks.size());
    }

    @Test
    public void testBackoffMsUpdatesReadTaskExpiry() throws Exception {
        Properties props = setUpProperties();
        props.put(KafkaRestConfig.CONSUMER_ITERATOR_BACKOFF_MS_CONFIG, "1000");
        config = new KafkaRestConfig(props, new SystemTime());
        consumerManager = new KafkaConsumerManager(config, consumerFactory);
        consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST, groupName);
        bootstrapConsumer(consumer);
        consumerManager.readRecords(groupName, consumer.cid(), BinaryKafkaConsumerState.class, -1, Long.MAX_VALUE,
                new ConsumerReadCallback<byte[], byte[]>() {
                    @Override
                    public void onCompletion(List<? extends ConsumerRecord<byte[], byte[]>> records, RestException e) {
                        actualException = e;
                        actualRecords = records;
                        sawCallback = true;
                    }
                });

        Thread.sleep(100);
        KafkaConsumerManager.RunnableReadTask readTask = consumerManager.delayedReadTasks.peek();
        if (readTask == null) {
            fail("Could not get read task in time. It should not be null");
        }
        long delay = readTask.getDelay(TimeUnit.MILLISECONDS);
        assertTrue(delay < 1000);
        assertTrue(delay > 700);
    }

    @Test
    public void testConsumerExpirationIsUpdated() throws Exception {
        bootstrapConsumer(consumer);
        KafkaConsumerState state = consumerManager.getConsumerInstance(groupName, consumer.cid());
        long initialExpiration = state.expiration;
        consumerManager.readRecords(groupName, consumer.cid(), BinaryKafkaConsumerState.class, -1, Long.MAX_VALUE,
                new ConsumerReadCallback<byte[], byte[]>() {
                    @Override
                    public void onCompletion(List<? extends ConsumerRecord<byte[], byte[]>> records, RestException e) {
                        actualException = e;
                        actualRecords = records;
                        sawCallback = true;
                    }
                });
        Thread.sleep(100);
        assertTrue(state.expiration > initialExpiration);
        initialExpiration = state.expiration;
        awaitRead();
        assertTrue("Callback failed to fire", sawCallback);
        assertTrue(state.expiration > initialExpiration);
        initialExpiration = state.expiration;

        consumerManager.commitOffsets(groupName, consumer.cid(), null, null, new KafkaConsumerManager.CommitCallback() {
            @Override
            public void onCompletion(List<TopicPartitionOffset> offsets, Exception e) {
                sawCallback = true;

                actualException = e;
                actualOffsets = offsets;
            }
        }).get();
        assertTrue(state.expiration > initialExpiration);
    }

    private void awaitRead() throws InterruptedException {
        Thread.sleep((long) (Integer.parseInt(KafkaRestConfig.CONSUMER_REQUEST_TIMEOUT_MS_DEFAULT) * 1.10));
    }

    @Ignore
    @Test
    public void testReadRecordsPopulatesDelayedReadTaskWhenExecutorFull() throws Exception {
        Properties props = setUpProperties();
        props.setProperty(KafkaRestConfig.CONSUMER_ITERATOR_BACKOFF_MS_CONFIG, "1");
        config = new KafkaRestConfig(props, new SystemTime());
        consumerManager = new KafkaConsumerManager(config, consumerFactory);
        MockConsumer consumer1 = new MockConsumer<>(OffsetResetStrategy.EARLIEST, "a");
        MockConsumer consumer2 = new MockConsumer<>(OffsetResetStrategy.EARLIEST, "b");
        MockConsumer consumer3 = new MockConsumer<>(OffsetResetStrategy.EARLIEST, "c");
        capturedConsumerConfig = Capture.newInstance();
        EasyMock.expect(consumerFactory.createConsumer(EasyMock.capture(capturedConsumerConfig)))
                .andReturn(consumer1).andReturn(consumer2).andReturn(consumer3);
        EasyMock.replay(consumerFactory);
        bootstrapConsumer(consumer1, false);
        bootstrapConsumer(consumer2, false);
        bootstrapConsumer(consumer3, false);

        ConsumerReadCallback callback = new ConsumerReadCallback<byte[], byte[]>() {
            @Override
            public void onCompletion(List<? extends ConsumerRecord<byte[], byte[]>> records, RestException e) {
                actualException = e;
                actualRecords = records;
                sawCallback = true;
            }
        };
        consumerManager.readRecords("a", consumer1.cid(), BinaryKafkaConsumerState.class, -1, Long.MAX_VALUE, callback);
        // should go over the consumer.threads threshold and be rejected. Rejected tasks are delayed between 25-75ms
        consumerManager.readRecords("a", consumer2.cid(), BinaryKafkaConsumerState.class, -1, Long.MAX_VALUE, callback);
        Thread.sleep(10000);
        for (Object a : consumerManager.delayedReadTasks) {
            long delayMs = ((KafkaConsumerManager.RunnableReadTask) a).getDelay(TimeUnit.MILLISECONDS);
            assertTrue(delayMs > 20);
            assertTrue(delayMs < 75);
        }
        assertEquals(1, consumerManager.delayedReadTasks.size());
        consumerManager.readRecords("c", consumer3.cid(), BinaryKafkaConsumerState.class, -1, Long.MAX_VALUE, callback);
        assertEquals(2, consumerManager.delayedReadTasks.size());
        for (Object a : consumerManager.delayedReadTasks) {
            long delayMs = ((KafkaConsumerManager.RunnableReadTask) a).getDelay(TimeUnit.MILLISECONDS);
            assertTrue(delayMs > 20);
            assertTrue(delayMs < 75);
        }
    }

    private List<ConsumerRecord<byte[], byte[]>> bootstrapConsumer(final MockConsumer<byte[], byte[]> consumer) {
        return bootstrapConsumer(consumer, true);
    }

    /**
     * Subscribes a consumer to a topic and schedules a poll task
     */
    private List<ConsumerRecord<byte[], byte[]>> bootstrapConsumer(final MockConsumer<byte[], byte[]> consumer, boolean toExpectCreate) {
        final List<ConsumerRecord<byte[], byte[]>> referenceRecords
                = Arrays.<ConsumerRecord<byte[], byte[]>>asList(
                new BinaryConsumerRecord(topicName, "k1".getBytes(), "v1".getBytes(), 0, 0),
                new BinaryConsumerRecord(topicName, "k2".getBytes(), "v2".getBytes(), 0, 1),
                new BinaryConsumerRecord(topicName, "k3".getBytes(), "v3".getBytes(), 0, 2)
        );

        if (toExpectCreate)
            expectCreate(consumer);
        consumer.schedulePollTask(new Runnable() {
            @Override
            public void run() {
                consumer.addRecord(new org.apache.kafka.clients.consumer.ConsumerRecord<>(topicName, 0, 0, "k1".getBytes(), "v1".getBytes()));
                consumer.addRecord(new org.apache.kafka.clients.consumer.ConsumerRecord<>(topicName, 0, 1, "k2".getBytes(), "v2".getBytes()));
                consumer.addRecord(new org.apache.kafka.clients.consumer.ConsumerRecord<>(topicName, 0, 2, "k3".getBytes(), "v3".getBytes()));
            }
        });

        String cid = consumerManager.createConsumer(
                consumer.groupName, new ConsumerInstanceConfig(EmbeddedFormat.BINARY));

        consumer.cid(cid);
        consumerManager.subscribe(consumer.groupName, cid, new ConsumerSubscriptionRecord(Collections.singletonList(topicName), null));
        consumer.rebalance(Collections.singletonList(new TopicPartition(topicName, 0)));
        consumer.updateBeginningOffsets(Collections.singletonMap(new TopicPartition(topicName, 0), 0L));

        return referenceRecords;
    }

    private void readFromDefault(String cid) throws InterruptedException, ExecutionException {
        consumerManager.readRecords(groupName, cid, BinaryKafkaConsumerState.class, -1, Long.MAX_VALUE,
            new ConsumerReadCallback<byte[], byte[]>() {
              @Override
              public void onCompletion(List<? extends ConsumerRecord<byte[], byte[]>> records, RestException e) {
                actualException = e;
                actualRecords = records;
                sawCallback = true;
              }
            });
    }

    private List<ConsumerRecord<byte[], byte[]>> schedulePoll() {
        return schedulePoll(0);
    }

    private List<ConsumerRecord<byte[], byte[]>> schedulePoll(final int fromOffset) {
        consumer.schedulePollTask(new Runnable() {
          @Override
          public void run() {
            consumer.addRecord(KafkaConsumerManagerTest.this.record(fromOffset));
            consumer.addRecord(KafkaConsumerManagerTest.this.record(fromOffset + 1));
            consumer.addRecord(KafkaConsumerManagerTest.this.record(fromOffset + 2));
          }
        });
        return Arrays.asList(
            (ConsumerRecord<byte[], byte[]>) binaryConsumerRecord(fromOffset),
            binaryConsumerRecord(fromOffset + 1),
            binaryConsumerRecord(fromOffset + 2)
        );
    }

    private BinaryConsumerRecord binaryConsumerRecord(int offset) {
        return new BinaryConsumerRecord(
                topicName,
                String.format("k%d", offset).getBytes(),
                String.format("v%d", offset).getBytes(),
                0,
                offset
        );
    }

    private org.apache.kafka.clients.consumer.ConsumerRecord<byte[], byte[]> record(int offset) {
        return new org.apache.kafka.clients.consumer.ConsumerRecord<>(
                topicName,
                0,
                offset,
                String.format("k%d", offset).getBytes(),
                String.format("v%d", offset).getBytes());
    }
}
