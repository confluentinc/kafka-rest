package io.confluent.kafkarest.integration;

import io.confluent.kafkarest.entities.ProduceRequest;
import io.confluent.kafkarest.entities.ProduceResponse;
import io.confluent.kafkarest.entities.Topic;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
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
import static org.junit.Assert.assertArrayEquals;

public class ProducerTest extends ClusterTestHarness {
    private static final String topicName = "topic1";
    private static final Topic topic = new Topic(topicName, 1);

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        TestUtils.createTopic(zkClient, topicName, 1, 1, JavaConversions.asScalaIterable(this.servers).toSeq(), new Properties());
    }

    @Test
    public void testProduceToTopic() {
        ProduceRequest payload = new ProduceRequest();
        payload.setRecords(Arrays.asList(
                new ProduceRequest.ProduceRecord("key".getBytes(), "value".getBytes()),
                new ProduceRequest.ProduceRecord("key2".getBytes(), "value2".getBytes()),
                new ProduceRequest.ProduceRecord("key3".getBytes(), "value3".getBytes()),
                new ProduceRequest.ProduceRecord("key4".getBytes(), "value4".getBytes())
        ));
        final ProduceResponse response = request("/topics/" + topicName)
                .post(Entity.entity(payload, MediaType.APPLICATION_JSON_TYPE), ProduceResponse.class);
        assertEquals(
                Arrays.asList(
                        new ProduceResponse.PartitionOffset(0,3)
                ),
                response.getOffsets()
        );

        assertTopicContains(payload.getRecords());
    }

    @Test
    public void testProduceToInvalidTopic() {
        ProduceRequest payload = new ProduceRequest();
        payload.setRecords(Arrays.asList(
                new ProduceRequest.ProduceRecord("key".getBytes(), "value".getBytes())
        ));
        final Response response = request("/topics/topicdoesnotexist")
                .post(Entity.entity(payload, MediaType.APPLICATION_JSON_TYPE));
        assertEquals(Response.Status.NOT_FOUND.getStatusCode(), response.getStatus());
    }


    // Consumes messages from Kafka to verify they match the inputs
    private void assertTopicContains(List<ProduceRequest.ProduceRecord> records) {
        ConsumerConnector consumer = Consumer.createJavaConsumerConnector(
                new ConsumerConfig(TestUtils.createConsumerProperties(zkConnect, "testgroup", "consumer0", 200))
        );
        Map<String,Integer> topicCountMap = new HashMap<>();
        topicCountMap.put(topicName, 1);
        Map<String, List<KafkaStream<byte[], byte[]>>> streams = consumer.createMessageStreams(topicCountMap);
        KafkaStream<byte[], byte[]> stream = streams.get(topicName).get(0);
        ConsumerIterator<byte[], byte[]> it = stream.iterator();
        for(int i = 0; i < records.size(); i++) {
            MessageAndMetadata<byte[], byte[]> msg = it.next();
            assertArrayEquals(records.get(i).getKey(), msg.key());
            assertArrayEquals(records.get(i).getValue(), msg.message());
        }
        consumer.shutdown();
    }
}
