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

import io.confluent.kafkarest.entities.ConsumerOffsetCommitRequest;
import io.confluent.kafkarest.entities.ConsumerRecord;
import io.confluent.kafkarest.entities.ConsumerSubscriptionRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.easymock.EasyMockRunner;
import org.easymock.Mock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import io.confluent.kafkarest.KafkaRestConfig;
import io.confluent.kafkarest.MetadataObserver;
import io.confluent.kafkarest.entities.BinaryConsumerRecord;
import io.confluent.kafkarest.entities.ConsumerInstanceConfig;
import io.confluent.kafkarest.entities.EmbeddedFormat;
import io.confluent.kafkarest.entities.TopicPartitionOffset;
import io.confluent.kafkarest.mock.MockTime;
import io.confluent.rest.RestConfigException;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests basic create/read/commit/delete functionality of ConsumerManager. This only exercises the
 * functionality for binary data because it uses a mock consumer that only works with byte[] data.
 */
@RunWith(EasyMockRunner.class)
public class KafkaConsumerManagerTest {

    private Properties properties;
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
        properties = new Properties();
        properties.setProperty(KafkaRestConfig.BOOTSTRAP_SERVERS_CONFIG, "PLAINTEXT://hostname:9092");
        properties.setProperty(KafkaRestConfig.CONSUMER_REQUEST_MAX_BYTES_CONFIG, "1024");
        // This setting supports the testConsumerOverrides test. It is otherwise benign and should
        // not affect other tests.
        properties.setProperty("consumer." + ConsumerConfig.EXCLUDE_INTERNAL_TOPICS_CONFIG, "false");

        setUp(properties);
    }

    public void setUp(Properties properties) throws RestConfigException {
        config = new KafkaRestConfig(properties, new MockTime());
        consumerManager = new KafkaConsumerManager(config, consumerFactory);
        consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
    }

    @After
    public void tearDown() {
        consumerManager.shutdown();
    }

    private void expectCreate() {
        capturedConsumerConfig = Capture.newInstance();
        EasyMock.expect(consumerFactory.createConsumer(EasyMock.capture(capturedConsumerConfig)))
                .andReturn(consumer);
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
     * consumer.request.timeout.ms should not modify how long the proxy waits until returning a response
     */
    @Test
    public void testConsumerRequestTimeoutDoesNotModifyProxyResponseTime() throws Exception {
        properties.setProperty(KafkaRestConfig.CONSUMER_REQUEST_TIMEOUT_MS_CONFIG, "2500");
        setUp(properties);
        expectCreate();
        EasyMock.replay(mdObserver, consumerFactory);

        String cid = consumerManager.createConsumer(
                groupName, new ConsumerInstanceConfig(EmbeddedFormat.BINARY));
        consumerManager.subscribe(groupName, cid, new ConsumerSubscriptionRecord(Collections.singletonList(topicName), null));

        readFromDefault(cid);

        assertTrue("Callback failed to fire", sawCallback);
        assertNull("No exception in callback", actualException);
        // should wait default time
        assertEquals(config.getInt(KafkaRestConfig.PROXY_FETCH_MAX_WAIT_MS_CONFIG),
                Integer.parseInt(KafkaRestConfig.PROXY_FETCH_MAX_WAIT_MS_DEFAULT));
        assertEquals(config.getInt(KafkaRestConfig.PROXY_FETCH_MAX_WAIT_MS_CONFIG),
                config.getTime().milliseconds());
    }

    /**
     * Response should return no sooner than KafkaRestConfig.PROXY_FETCH_MAX_WAIT_MS_CONFIG
     */
    @Test
    public void testConsumerWaitMs() throws Exception {
        properties.setProperty(KafkaRestConfig.PROXY_FETCH_MAX_WAIT_MS_CONFIG, "139");
        setUp(properties);
        expectCreate();
        EasyMock.replay(mdObserver, consumerFactory);

        String cid = consumerManager.createConsumer(
                groupName, new ConsumerInstanceConfig(EmbeddedFormat.BINARY));
        consumerManager.subscribe(groupName, cid, new ConsumerSubscriptionRecord(Collections.singletonList(topicName), null));

        readFromDefault(cid);
        assertTrue("Callback failed to fire", sawCallback);
        assertNull("No exception in callback", actualException);
        assertEquals(config.getInt(KafkaRestConfig.PROXY_FETCH_MAX_WAIT_MS_CONFIG),
                config.getTime().milliseconds());
    }

    /**
     * When min.bytes is not fulfilled, we should return after wait.ms
     * When min.bytes is fulfilled, we should return immediately
     */
    @Test
    public void testConsumerWaitMsAndMinBytes() throws Exception {
        properties.setProperty(KafkaRestConfig.PROXY_FETCH_MAX_WAIT_MS_CONFIG, "1303");
        properties.setProperty(KafkaRestConfig.PROXY_FETCH_MIN_BYTES_CONFIG, "5");
        setUp(properties);
        expectCreate();

        EasyMock.replay(mdObserver, consumerFactory);

        String cid = consumerManager.createConsumer(
                groupName, new ConsumerInstanceConfig(EmbeddedFormat.BINARY));
        consumerManager.subscribe(groupName, cid, new ConsumerSubscriptionRecord(Collections.singletonList(topicName), null));
        consumer.rebalance(Collections.singletonList(new TopicPartition(topicName, 0)));
        consumer.updateBeginningOffsets(Collections.singletonMap(new TopicPartition(topicName, 0), 0L));

        long startTime = System.currentTimeMillis();
        readFromDefault(cid);
        assertTrue("Callback failed to fire", sawCallback);
        assertNull("No exception in callback", actualException);
        assertTrue("Records returned not empty", actualRecords.isEmpty());
        long estimatedTime = System.currentTimeMillis() - startTime;
        int expectedWaitMs = config.getInt(KafkaRestConfig.PROXY_FETCH_MAX_WAIT_MS_CONFIG);
        assertEquals(expectedWaitMs,
                config.getTime().milliseconds());
        assertTrue(estimatedTime > expectedWaitMs); // should not have returned any earlier, since min.bytes not fulfilled


        sawCallback = false;
        final List<ConsumerRecord<byte[], byte[]>> referenceRecords = schedulePoll();
        startTime = System.currentTimeMillis();
        readFromDefault(cid);
        estimatedTime = System.currentTimeMillis() - startTime;

        assertTrue(estimatedTime < expectedWaitMs); // should have returned immediately
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
        properties.setProperty(KafkaRestConfig.PROXY_FETCH_MIN_BYTES_CONFIG, Integer.toString(sampleRecordSize));
        properties.setProperty(KafkaRestConfig.PROXY_FETCH_MAX_BYTES_CONFIG, Integer.toString(sampleRecordSize * 10));
        setUp(properties);
        final List<ConsumerRecord<byte[], byte[]>> scheduledRecords = schedulePoll();
        final List<ConsumerRecord<byte[], byte[]>> referenceRecords = Arrays.asList(scheduledRecords.get(0), scheduledRecords.get(1), scheduledRecords.get(2));
        schedulePoll(3);

        expectCreate();
        EasyMock.replay(mdObserver, consumerFactory);
        String cid = consumerManager.createConsumer(
                groupName, new ConsumerInstanceConfig(EmbeddedFormat.BINARY));
        consumerManager.subscribe(groupName, cid, new ConsumerSubscriptionRecord(Collections.singletonList(topicName), null));
        consumer.rebalance(Collections.singletonList(new TopicPartition(topicName, 0)));
        consumer.updateBeginningOffsets(Collections.singletonMap(new TopicPartition(topicName, 0), 0L));

        readFromDefault(cid);

        assertTrue("Callback failed to fire", sawCallback);
        assertNull("No exception in callback", actualException);
        assertEquals("Records returned not as expected", referenceRecords, actualRecords);
    }

    @Test
    public void testConsumeMinBytesIsOverridablePerConsumer() throws Exception {
        BinaryConsumerRecord sampleRecord = binaryConsumerRecord(0);
        int sampleRecordSize = sampleRecord.getKey().length + sampleRecord.getValue().length;
        properties.setProperty(KafkaRestConfig.PROXY_FETCH_MIN_BYTES_CONFIG, Integer.toString(sampleRecordSize * 5));
        properties.setProperty(KafkaRestConfig.PROXY_FETCH_MAX_BYTES_CONFIG, Integer.toString(sampleRecordSize * 6));
        setUp(properties);
        final List<ConsumerRecord<byte[], byte[]>> scheduledRecords = schedulePoll();
        // global settings would make the consumer call poll twice and get more than 3 records,
        // overridden settings should make him poll once since the min bytes will be reached
        final List<ConsumerRecord<byte[], byte[]>> referenceRecords = Arrays.asList(scheduledRecords.get(0),
                scheduledRecords.get(1),
                scheduledRecords.get(2));
        schedulePoll(3);

        expectCreate();
        EasyMock.replay(mdObserver, consumerFactory);
        ConsumerInstanceConfig config = new ConsumerInstanceConfig(EmbeddedFormat.BINARY);
        // we expect three records to be returned since the setting is overridden and poll() wont be called a second time
        config.setResponseMinBytes(Integer.toString(sampleRecordSize * 2));
        String cid = consumerManager.createConsumer(
                groupName, config);
        consumerManager.subscribe(groupName, cid, new ConsumerSubscriptionRecord(Collections.singletonList(topicName), null));
        consumer.rebalance(Collections.singletonList(new TopicPartition(topicName, 0)));
        consumer.updateBeginningOffsets(Collections.singletonMap(new TopicPartition(topicName, 0), 0L));

        readFromDefault(cid);

        assertTrue("Callback failed to fire", sawCallback);
        assertNull("No exception in callback", actualException);
        assertEquals("Records returned not as expected", referenceRecords, actualRecords);
    }

    /**
     * Response should return no sooner than the overridden PROXY_FETCH_MAX_WAIT_MS_CONFIG
     */
    @Test
    public void testConsumerWaitMsIsOverriddablePerConsumer() throws Exception {
        properties.setProperty(KafkaRestConfig.PROXY_FETCH_MAX_WAIT_MS_CONFIG, "139");
        setUp(properties);
        expectCreate();
        EasyMock.replay(mdObserver, consumerFactory);

        ConsumerInstanceConfig consumerConfig = new ConsumerInstanceConfig(EmbeddedFormat.BINARY);
        consumerConfig.setRequestWaitMs("111");
        String cid = consumerManager.createConsumer(groupName, consumerConfig);
        consumerManager.subscribe(groupName, cid, new ConsumerSubscriptionRecord(Collections.singletonList(topicName), null));

        readFromDefault(cid);
        assertTrue("Callback failed to fire", sawCallback);
        assertNull("No exception in callback", actualException);
        assertEquals(111, config.getTime().milliseconds());
    }


    @Test
    public void testConsumerNormalOps() throws InterruptedException, ExecutionException {
        expectCreate();
        final List<ConsumerRecord<byte[], byte[]>> referenceRecords = schedulePoll();

        EasyMock.replay(mdObserver, consumerFactory);

        String cid = consumerManager.createConsumer(
                groupName, new ConsumerInstanceConfig(EmbeddedFormat.BINARY));
        consumerManager.subscribe(groupName, cid, new ConsumerSubscriptionRecord(Collections.singletonList(topicName), null));
        consumer.rebalance(Collections.singletonList(new TopicPartition(topicName, 0)));
        consumer.updateBeginningOffsets(Collections.singletonMap(new TopicPartition(topicName, 0), 0L));

        sawCallback = false;
        actualException = null;
        actualRecords = null;
        readFromDefault(cid);
        assertTrue("Callback failed to fire", sawCallback);
        assertNull("No exception in callback", actualException);
        assertEquals("Records returned not as expected", referenceRecords, actualRecords);
        // With # of bytes in messages < max bytes per response and with a backoff that divides the timeout evenly,
        // this should finish just at the per-request timeout (because the timeout perfectly coincides with a scheduled
        // iteration when using the default settings).
        assertEquals(config.getInt(KafkaRestConfig.CONSUMER_REQUEST_TIMEOUT_MS_CONFIG),
                config.getTime().milliseconds());

        sawCallback = false;
        actualException = null;
        actualOffsets = null;
        ConsumerOffsetCommitRequest commitRequest = null; // Commit all offsets
        consumerManager.commitOffsets(groupName, cid, null, commitRequest, new KafkaConsumerManager.CommitCallback() {
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

        consumerManager.deleteConsumer(groupName, cid);

        EasyMock.verify(mdObserver, consumerFactory);
    }

    private void readFromDefault(String cid) throws InterruptedException, ExecutionException {
        consumerManager.readRecords(groupName, cid, BinaryKafkaConsumerState.class, -1, Long.MAX_VALUE,
                (KafkaConsumerManager.ReadCallback<byte[], byte[]>) (records, e) -> {
                    actualException = e;
                    actualRecords = records;
                    sawCallback = true;
        }).get();
    }

    private List<ConsumerRecord<byte[], byte[]>> schedulePoll() {
        return schedulePoll(0);
    }

    private List<ConsumerRecord<byte[], byte[]>> schedulePoll(int fromOffset) {
        consumer.schedulePollTask(() -> {
            consumer.addRecord(record(fromOffset));
            consumer.addRecord(record(fromOffset + 1));
            consumer.addRecord(record(fromOffset + 2));
        });
        return Arrays.asList(
                binaryConsumerRecord(fromOffset),
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

    private org.apache.kafka.clients.consumer.ConsumerRecord record(int offset) {
        return new org.apache.kafka.clients.consumer.ConsumerRecord<>(
                topicName,
                0,
                offset,
                String.format("k%d", offset).getBytes(),
                String.format("v%d", offset).getBytes());
    }
}
