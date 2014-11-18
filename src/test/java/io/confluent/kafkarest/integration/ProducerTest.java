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
package io.confluent.kafkarest.integration;

import io.confluent.kafkarest.entities.*;
import kafka.consumer.*;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import kafka.utils.TestUtils;
import org.junit.Before;
import org.junit.Test;
import scala.collection.JavaConversions;

import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.*;

import static org.junit.Assert.assertEquals;

public class ProducerTest extends ClusterTestHarness {
    private static final String topicName = "topic1";
    private static final Topic topic = new Topic(topicName, 1);


    // Produce to topic inputs & results

    private final List<TopicProduceRecord> topicRecordsWithKeys = Arrays.asList(
            new TopicProduceRecord("key".getBytes(), "value".getBytes()),
            new TopicProduceRecord("key".getBytes(), "value2".getBytes()),
            new TopicProduceRecord("key".getBytes(), "value3".getBytes()),
            new TopicProduceRecord("key".getBytes(), "value4".getBytes())
    );
    private final List<PartitionOffset> partitionOffsetsWithKeys = Arrays.asList(
            new PartitionOffset(1,3)
    );

    private final List<TopicProduceRecord> topicRecordsWithPartitions = Arrays.asList(
            new TopicProduceRecord("value".getBytes(), 0),
            new TopicProduceRecord("value2".getBytes(), 1),
            new TopicProduceRecord("value3".getBytes(), 0),
            new TopicProduceRecord("value4".getBytes(), 2)
    );
    private final List<PartitionOffset> partitionOffsetsWithPartitions = Arrays.asList(
            new PartitionOffset(0,1),
            new PartitionOffset(1,0),
            new PartitionOffset(2,0)
    );

    private final List<TopicProduceRecord> topicRecordsWithPartitionsAndKeys = Arrays.asList(
            new TopicProduceRecord("key".getBytes(), "value".getBytes(), 0),
            new TopicProduceRecord("key2".getBytes(), "value2".getBytes(), 1),
            new TopicProduceRecord("key3".getBytes(), "value3".getBytes(), 1),
            new TopicProduceRecord("key4".getBytes(), "value4".getBytes(), 2)
    );
    private final List<PartitionOffset> partitionOffsetsWithPartitionsAndKeys = Arrays.asList(
            new PartitionOffset(0,0),
            new PartitionOffset(1,1),
            new PartitionOffset(2,0)
    );

    // Produce to partition inputs & results
    private final List<ProduceRecord> partitionRecordsOnlyValues = Arrays.asList(
            new ProduceRecord("value".getBytes()),
            new ProduceRecord("value2".getBytes()),
            new ProduceRecord("value3".getBytes()),
            new ProduceRecord("value4".getBytes())
    );
    private final PartitionOffset producePartitionOffsetOnlyValues = new PartitionOffset(0,3);

    private final List<ProduceRecord> partitionRecordsWithKeys = Arrays.asList(
            new ProduceRecord("key".getBytes(), "value".getBytes()),
            new ProduceRecord("key".getBytes(), "value2".getBytes()),
            new ProduceRecord("key".getBytes(), "value3".getBytes()),
            new ProduceRecord("key".getBytes(), "value4".getBytes())
    );
    private final PartitionOffset producePartitionOffsetWithKeys = new PartitionOffset(0,3);


    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        final int numPartitions = 3;
        final int replicationFactor = 1;
        TestUtils.createTopic(zkClient, topicName, numPartitions, replicationFactor, JavaConversions.asScalaIterable(this.servers).toSeq(), new Properties());
    }

    private void testProduceToTopic(List<TopicProduceRecord> records, List<PartitionOffset> offsetResponses) {
        TopicProduceRequest payload = new TopicProduceRequest();
        payload.setRecords(records);
        final ProduceResponse response = request("/topics/" + topicName)
                .post(Entity.entity(payload, MediaType.APPLICATION_JSON_TYPE), ProduceResponse.class);
        assertEquals(offsetResponses, response.getOffsets());
        assertTopicContains(payload.getRecords(), null);
    }

    @Test
    public void testProduceToTopicWithKeys() {
        testProduceToTopic(topicRecordsWithKeys, partitionOffsetsWithKeys);
    }

    @Test
    public void testProduceToTopicWithPartitions() {
        testProduceToTopic(topicRecordsWithPartitions, partitionOffsetsWithPartitions);
    }

    @Test
    public void testProduceToTopicWithPartitionsAndKeys() {
        testProduceToTopic(topicRecordsWithPartitionsAndKeys, partitionOffsetsWithPartitionsAndKeys);
    }

    @Test
    public void testProduceToInvalidTopic() {
        TopicProduceRequest payload = new TopicProduceRequest();
        payload.setRecords(Arrays.asList(
                new TopicProduceRecord("key".getBytes(), "value".getBytes())
        ));
        final Response response = request("/topics/topicdoesnotexist")
                .post(Entity.entity(payload, MediaType.APPLICATION_JSON_TYPE));
        assertEquals(Response.Status.NOT_FOUND.getStatusCode(), response.getStatus());
    }



    private void testProduceToPartition(List<ProduceRecord> records, PartitionOffset offsetResponse) {
        PartitionProduceRequest payload = new PartitionProduceRequest();
        payload.setRecords(records);
        final PartitionOffset response = request("/topics/" + topicName + "/partitions/0")
                .post(Entity.entity(payload, MediaType.APPLICATION_JSON_TYPE), PartitionOffset.class);
        assertEquals(offsetResponse, response);
        assertTopicContains(payload.getRecords(), (Integer)0);
    }

    @Test
    public void testProduceToPartitionOnlyValues() {
        testProduceToPartition(partitionRecordsOnlyValues, producePartitionOffsetOnlyValues);
    }

    @Test
    public void testProduceToPartitionWithKeys() {
        testProduceToPartition(partitionRecordsWithKeys, producePartitionOffsetWithKeys);
    }




    // Consumes messages from Kafka to verify they match the inputs. Optionally add a partition to only examine that partition
    private void assertTopicContains(List<? extends ProduceRecord> records, Integer partition) {
        ConsumerConnector consumer = Consumer.createJavaConsumerConnector(
                new ConsumerConfig(TestUtils.createConsumerProperties(zkConnect, "testgroup", "consumer0", 200))
        );
        Map<String,Integer> topicCountMap = new HashMap<>();
        topicCountMap.put(topicName, 1);
        Map<String, List<KafkaStream<byte[], byte[]>>> streams = consumer.createMessageStreams(topicCountMap);
        KafkaStream<byte[], byte[]> stream = streams.get(topicName).get(0);
        ConsumerIterator<byte[], byte[]> it = stream.iterator();
        Set<String> msgSet = new TreeSet<>();
        for(int i = 0; i < records.size(); i++) {
            MessageAndMetadata<byte[],byte[]> data = it.next();
            if (partition == null || data.partition() == partition)
                msgSet.add(EntityUtils.encodeBase64Binary(data.message()));
        }
        consumer.shutdown();

        Set<String> refMsgSet = new TreeSet<>();
        for(ProduceRecord rec : records)
            refMsgSet.add(EntityUtils.encodeBase64Binary(rec.getValue()));
        assertEquals(msgSet, refMsgSet);
    }
}
