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
import java.util.concurrent.Future;

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
        config = new KafkaRestConfig(setUpProperties(), new MockTime());
        consumerManager = new KafkaConsumerManager(config, consumerFactory);
        consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
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
        // This setting supports the testConsumerOverrides test. It is otherwise benign and should
        // not affect other tests.
        props.setProperty("consumer." + ConsumerConfig.EXCLUDE_INTERNAL_TOPICS_CONFIG, "false");

        return props;
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

    @Test
    public void testConsumerNormalOps() throws InterruptedException, ExecutionException, RestConfigException {
        config = new KafkaRestConfig(setUpProperties(), new SystemTime());
        consumerManager = new KafkaConsumerManager(config, consumerFactory);
        consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        // Tests create instance, read, and delete
        final List<ConsumerRecord<byte[], byte[]>> referenceRecords
                = Arrays.<ConsumerRecord<byte[], byte[]>>asList(
                new BinaryConsumerRecord(topicName, "k1".getBytes(), "v1".getBytes(), 0, 0),
                new BinaryConsumerRecord(topicName, "k2".getBytes(), "v2".getBytes(), 0, 1),
                new BinaryConsumerRecord(topicName, "k3".getBytes(), "v3".getBytes(), 0, 2)
        );

        expectCreate();
        consumer.schedulePollTask(new Runnable() {
            @Override
            public void run() {
                consumer.addRecord(new org.apache.kafka.clients.consumer.ConsumerRecord<>(topicName, 0, 0, "k1".getBytes(), "v1".getBytes()));
                consumer.addRecord(new org.apache.kafka.clients.consumer.ConsumerRecord<>(topicName, 0, 1, "k2".getBytes(), "v2".getBytes()));
                consumer.addRecord(new org.apache.kafka.clients.consumer.ConsumerRecord<>(topicName, 0, 2, "k3".getBytes(), "v3".getBytes()));
            }
        });
        EasyMock.replay(mdObserver, consumerFactory);

        String cid = consumerManager.createConsumer(
                groupName, new ConsumerInstanceConfig(EmbeddedFormat.BINARY));
        consumerManager.subscribe(groupName, cid, new ConsumerSubscriptionRecord(Collections.singletonList(topicName), null));
        consumer.rebalance(Collections.singletonList(new TopicPartition(topicName, 0)));
        consumer.updateBeginningOffsets(Collections.singletonMap(new TopicPartition(topicName, 0), 0L));

        sawCallback = false;
        actualException = null;
        actualRecords = null;
        consumerManager.readRecords(groupName, cid, BinaryKafkaConsumerState.class, -1, Long.MAX_VALUE,
            new ConsumerReadCallback<byte[], byte[]>() {
            @Override
            public void onCompletion(List<? extends ConsumerRecord<byte[], byte[]>> records, RestException e) {
                actualException = e;
                actualRecords = records;
                sawCallback = true;
            }
        });
        Thread.sleep((long) (Integer.parseInt(KafkaRestConfig.CONSUMER_REQUEST_TIMEOUT_MS_DEFAULT) * 1.10));

        assertTrue("Callback failed to fire", sawCallback);
        assertNull("No exception in callback", actualException);
        assertEquals("Records returned not as expected", referenceRecords, actualRecords);
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
}
