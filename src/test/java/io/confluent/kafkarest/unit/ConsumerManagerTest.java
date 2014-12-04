/**
 * Copyright 2014 Confluent Inc.
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
 */
package io.confluent.kafkarest.unit;

import io.confluent.kafkarest.Config;
import io.confluent.kafkarest.ConfigurationException;
import io.confluent.kafkarest.ConsumerManager;
import io.confluent.kafkarest.MetadataObserver;
import io.confluent.kafkarest.entities.ConsumerInstanceConfig;
import io.confluent.kafkarest.entities.ConsumerRecord;
import io.confluent.kafkarest.entities.TopicPartitionOffset;
import io.confluent.kafkarest.mock.MockConsumerConnector;
import io.confluent.kafkarest.mock.MockTime;
import kafka.consumer.ConsumerConfig;
import kafka.javaapi.consumer.ConsumerConnector;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;

import javax.ws.rs.NotFoundException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;

public class ConsumerManagerTest {
    private Config config;
    private MetadataObserver mdObserver;
    private ConsumerManager.ConsumerFactory consumerFactory;
    private ConsumerManager consumerManager;

    private static final String groupName = "testgroup";
    private static final String topicName = "testtopic";

    @Before
    public void setUp() throws ConfigurationException {
        config = new Config();
        config.time = new MockTime();
        mdObserver = EasyMock.createMock(MetadataObserver.class);
        consumerFactory = EasyMock.createMock(ConsumerManager.ConsumerFactory.class);
        consumerManager = new ConsumerManager(config, mdObserver, consumerFactory);
    }

    private ConsumerConnector expectCreate(Map<String,List<Map<Integer,List<ConsumerRecord>>>> schedules) {
        ConsumerConnector consumer = new MockConsumerConnector(config.time, "testclient", schedules, Integer.parseInt(Config.DEFAULT_CONSUMER_ITERATOR_TIMEOUT_MS));
        EasyMock.expect(consumerFactory.createConsumer(EasyMock.<ConsumerConfig>anyObject()))
                .andReturn(consumer);
        return consumer;
    }

    @Test
    public void testConsumerNormalOps() throws InterruptedException, ExecutionException {
        // Tests create instance, read, and delete
        final List<ConsumerRecord> referenceRecords = Arrays.asList(
                new ConsumerRecord("k1".getBytes(), "v1".getBytes(), 0, 0),
                new ConsumerRecord("k2".getBytes(), "v2".getBytes(), 1, 0),
                new ConsumerRecord("k3".getBytes(), "v3".getBytes(), 2, 0)
        );
        Map<Integer,List<ConsumerRecord>> referenceSchedule = new HashMap<>();
        referenceSchedule.put(50, referenceRecords);

        Map<String,List<Map<Integer,List<ConsumerRecord>>>> schedules = new HashMap<>();
        schedules.put(topicName, Arrays.asList(referenceSchedule));

        expectCreate(schedules);
        EasyMock.expect(mdObserver.topicExists(topicName)).andReturn(true);

        EasyMock.replay(mdObserver, consumerFactory);

        String cid = consumerManager.createConsumer(groupName, new ConsumerInstanceConfig());
        consumerManager.readTopic(groupName, cid, topicName, new ConsumerManager.ReadCallback() {
            @Override
            public void onCompletion(List<ConsumerRecord> records, Exception e) {
                assertNull(e);
                assertEquals(referenceRecords, records);
            }
        }).get();
        // With # of messages < max per request, this should finish at the per-request timeout
        assertEquals(config.consumerRequestTimeoutMs, config.time.milliseconds());

        consumerManager.commitOffsets(groupName, cid, new ConsumerManager.CommitCallback() {
            @Override
            public void onCompletion(List<TopicPartitionOffset> offsets, Exception e) {
                assertNull(e);
                // Mock consumer doesn't handle offsets, so we just check we get some output for the right partitions
                assertNotNull(offsets);
                assertEquals(3, offsets.size());
            }
        }).get();

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

        String instanceId = consumerManager.createConsumer(groupName, new ConsumerInstanceConfig());
        readAndExpectImmediateNotFound(instanceId, invalidTopicName);

        EasyMock.verify(mdObserver, consumerFactory);
    }

    @Test(expected=NotFoundException.class)
    public void testDeleteInvalidConsumer() {
        consumerManager.deleteConsumer(groupName, "invalidinstance");
    }


    private void readAndExpectNoDataRequestTimeout(String cid) throws InterruptedException, ExecutionException {
        long started = config.time.milliseconds();
        consumerManager.readTopic(groupName, cid, topicName, new ConsumerManager.ReadCallback() {
            @Override
            public void onCompletion(List<ConsumerRecord> records, Exception e) {
                assertNull(e);
            }
        }).get();
        assertEquals(started + config.consumerRequestTimeoutMs, config.time.milliseconds());
    }

    // Not found for instance or topic
    private void readAndExpectImmediateNotFound(String cid, String topic) {
        Future future = consumerManager.readTopic(groupName, cid, topic, new ConsumerManager.ReadCallback() {
            @Override
            public void onCompletion(List<ConsumerRecord> records, Exception e) {
                assertNull(records);
                assertThat(e, instanceOf(NotFoundException.class));
            }
        });
        assertNull(future);
    }
}
