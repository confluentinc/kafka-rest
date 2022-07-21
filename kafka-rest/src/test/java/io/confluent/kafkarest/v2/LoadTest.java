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
package io.confluent.kafkarest.v2;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.google.protobuf.ByteString;
import io.confluent.kafkarest.ConsumerReadCallback;
import io.confluent.kafkarest.KafkaRestConfig;
import io.confluent.kafkarest.Time;
import io.confluent.kafkarest.entities.ConsumerInstanceConfig;
import io.confluent.kafkarest.SystemTime;
import io.confluent.kafkarest.entities.ConsumerRecord;
import io.confluent.kafkarest.entities.EmbeddedFormat;
import io.confluent.kafkarest.entities.v2.ConsumerSubscriptionRecord;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.easymock.EasyMockRunner;
import org.easymock.IExpectationSetters;
import org.easymock.Mock;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(EasyMockRunner.class)
public class LoadTest {

    private KafkaRestConfig config;
    @Mock
    private KafkaConsumerManager.KafkaConsumerFactory consumerFactory;

    private KafkaConsumerManager consumerManager;

    private static final String topicName = "testtopic";

    private Capture<Properties> capturedConsumerConfig;

    private long requestTimeoutMs = 1000;

    private Random random = new Random();

    class ConsumerTestRun {
        private final MockConsumer consumer;
        private final Time time;
      private ReentrantLock lock = new ReentrantLock();
      private Condition cond = lock.newCondition();
      private volatile boolean sawCallback = false;
      private volatile List<ConsumerRecord<byte[], byte[]>> actualRecords = null;
        private volatile Exception actualException;
        private ConsumerReadCallback callback;
        private int latestOffset = 0;
        private long readStartMs;

        ConsumerTestRun(MockConsumer consumer) {
            this(consumer, new SystemTime());
        }

        ConsumerTestRun(MockConsumer consumer, Time time) {
            this.consumer = consumer;
            this.time = time;
            this.readStartMs = Integer.MAX_VALUE;
            sawCallback = false;
            callback = new ConsumerReadCallback<byte[], byte[]>() {
                @Override
                public void onCompletion(List<ConsumerRecord<byte[], byte[]>> records, Exception e) {
                    lock.lock();
                    try {
                        sawCallback = true;
                        actualRecords = records;
                        actualException = e;
                        cond.signalAll();
                    } finally {
                        lock.unlock();
                    }
                }
            };
        }

        void bootstrap() {
            bootstrapConsumer(consumer);
        }

        void read() {
            assertNull(actualException);
            assertNull(actualRecords);
            assertFalse(sawCallback);
            schedulePoll();
            consumerManager.readRecords(consumer.groupName, consumer.cid(), BinaryKafkaConsumerState.class,
                    -1, Long.MAX_VALUE, callback);
            this.readStartMs = time.milliseconds();
        }

        void awaitRead() throws InterruptedException {
            lock.lock();
            try {
                while (!sawCallback) {
                    cond.await();
                }
            } finally {
                lock.unlock();
            }
        }

        void verifyRead() {
            assertTrue("Callback failed to fire", sawCallback);
            assertNull("There shouldn't be an exception in callback", actualException);
            List<ConsumerRecord<ByteString, ByteString>> expectedRecords = referenceRecords();
            assertEquals("Records returned not as expected", expectedRecords, actualRecords);

            lock.lock();
            try {
                sawCallback = false;
                actualRecords = null;
                actualException = null;
            } finally {
                lock.unlock();
            }
        }

        private List<ConsumerRecord<ByteString, ByteString>> referenceRecords() {
            return Arrays.asList(
                ConsumerRecord.create(
                    topicName, ByteString.copyFromUtf8("k1"), ByteString.copyFromUtf8("v1"), 0, latestOffset - 3),
                ConsumerRecord.create(
                    topicName, ByteString.copyFromUtf8("k2"), ByteString.copyFromUtf8("v2"), 0, latestOffset - 2),
                ConsumerRecord.create(
                    topicName, ByteString.copyFromUtf8("k3"), ByteString.copyFromUtf8("v3"), 0, latestOffset - 1));
        }

        private void schedulePoll() {
            consumer.schedulePollTask(new Runnable() {
                @Override
                public void run() {
                    consumer.addRecord(new org.apache.kafka.clients.consumer.ConsumerRecord<>(topicName, 0, latestOffset, "k1".getBytes(), "v1".getBytes()));
                    consumer.addRecord(new org.apache.kafka.clients.consumer.ConsumerRecord<>(topicName, 0, latestOffset + 1, "k2".getBytes(), "v2".getBytes()));
                    consumer.addRecord(new org.apache.kafka.clients.consumer.ConsumerRecord<>(topicName, 0, latestOffset + 2, "k3".getBytes(), "v3".getBytes()));
                    latestOffset += 3;
                }
            });
        }
    }

    /**
     * Test continuous reads for 60 seconds. 50 consumers, separated in 5 consumer groups.
     */
    @Test
    public void testMultipleConsumerMultipleGroups() throws Exception {
        Properties props = new Properties();
        props.setProperty(KafkaRestConfig.BOOTSTRAP_SERVERS_CONFIG, "PLAINTEXT://hostname:9092");
        props.setProperty(KafkaRestConfig.CONSUMER_MAX_THREADS_CONFIG, "-1");
        props.setProperty(KafkaRestConfig.CONSUMER_REQUEST_TIMEOUT_MS_CONFIG, Long.toString(requestTimeoutMs));
        config = new KafkaRestConfig(props, new SystemTime());
        consumerManager = new KafkaConsumerManager(config, consumerFactory);
        List<ConsumerTestRun> consumers = new ArrayList<>();
        for (int group = 0; group < 5; group++) {
            for (int i = 0; i < 10; i++) {
                consumers.add(new ConsumerTestRun(
                        new MockConsumer<>(OffsetResetStrategy.EARLIEST, Integer.toString(group))));
            }
        }
        capturedConsumerConfig = Capture.newInstance();
        Properties properties = EasyMock.capture(capturedConsumerConfig);
        IExpectationSetters<Consumer> a = EasyMock.expect(consumerFactory.createConsumer(properties));
        Method andReturnInstance = a.getClass().getMethod("andReturn", Object.class);
        for (ConsumerTestRun run: consumers) {
            andReturnInstance.invoke(a, run.consumer);
        }
        EasyMock.replay(consumerFactory);

        for (ConsumerTestRun run: consumers) {
            run.bootstrap();
        }

        for (ConsumerTestRun run: consumers) {
            run.read();
        }
        for (int i = 0; i < 30; i++) {
            for (ConsumerTestRun run: consumers) {
                run.awaitRead();
                run.verifyRead();
                Thread.sleep(this.random.nextInt(5));
                run.read();
            }
        }
    }

    private void bootstrapConsumer(final MockConsumer<byte[], byte[]> consumer) {
        String cid = consumerManager.createConsumer(
                consumer.groupName, ConsumerInstanceConfig.create(EmbeddedFormat.BINARY));

        consumer.cid(cid);
        consumerManager.subscribe(consumer.groupName, cid, new ConsumerSubscriptionRecord(Collections.singletonList(topicName), null));
        consumer.rebalance(Collections.singletonList(new TopicPartition(topicName, 0)));
        consumer.updateBeginningOffsets(Collections.singletonMap(new TopicPartition(topicName, 0), 0L));
    }
}
