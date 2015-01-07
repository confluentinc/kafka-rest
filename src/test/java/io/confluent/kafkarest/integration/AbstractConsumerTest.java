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

import io.confluent.kafkarest.Errors;
import io.confluent.kafkarest.KafkaRestConfig;
import io.confluent.kafkarest.Versions;
import io.confluent.kafkarest.entities.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.ws.rs.client.Entity;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.Response;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import static io.confluent.kafkarest.TestUtils.assertErrorResponse;
import static io.confluent.kafkarest.TestUtils.assertOKResponse;
import static org.junit.Assert.*;

public class AbstractConsumerTest extends ClusterTestHarness {
    protected void produceMessages(List<ProducerRecord> records) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", bootstrapServers);
        props.setProperty("acks", "all");
        Producer producer = new KafkaProducer(props);
        for(ProducerRecord rec : records) {
            try {
                producer.send(rec).get();
            } catch (Exception e) {
                fail("Consumer test couldn't produce input messages to Kafka");
            }
        }
        producer.close();
    }

    // Need to start consuming before producing since consumer is instantiated internally and starts at latest offset
    protected String startConsumeMessages(String groupName, String topic) {
        return startConsumeMessages(groupName, topic, false);
    }
    protected String startConsumeMessages(String groupName, String topic, boolean expectFailure) {
        CreateConsumerInstanceResponse instanceResponse = request("/consumers/" + groupName)
                .post(Entity.entity(null, Versions.KAFKA_MOST_SPECIFIC_DEFAULT), CreateConsumerInstanceResponse.class);
        assertNotNull(instanceResponse.getInstanceId());
        assertTrue(instanceResponse.getInstanceId().length() > 0);
        assertTrue("Base URI should contain the consumer instance ID",
                instanceResponse.getBaseUri().contains(instanceResponse.getInstanceId()));

        // Start consuming. Since production hasn't started yet, this is expected to timeout.
        Response response = request(instanceResponse.getBaseUri() + "/topics/" + topic).get();
        if (expectFailure) {
            assertErrorResponse(Response.Status.NOT_FOUND, response,
                                Errors.TOPIC_NOT_FOUND_ERROR_CODE,
                                Errors.TOPIC_NOT_FOUND_MESSAGE,
                                Versions.KAFKA_MOST_SPECIFIC_DEFAULT);
        } else {
            assertOKResponse(response, Versions.KAFKA_MOST_SPECIFIC_DEFAULT);
            List<ConsumerRecord> consumed = response.readEntity(new GenericType<List<ConsumerRecord>>() {});
            assertEquals(0, consumed.size());
        }

        return instanceResponse.getBaseUri();
    }

    protected void consumeMessages(String instanceUri, String topic, List<ProducerRecord> records) {
        Response response = request(instanceUri + "/topics/" + topic).get();
        assertOKResponse(response, Versions.KAFKA_MOST_SPECIFIC_DEFAULT);
        List<ConsumerRecord> consumed = response.readEntity(new GenericType<List<ConsumerRecord>>() {});
        assertEquals(records.size(), consumed.size());

        // Since this is used for unkeyed messages, this can't rely on ordering of messages
        Set<String> inputSet = new HashSet<String>();
        for(ProducerRecord rec : records)
            inputSet.add(
                    (rec.key() == null ? "null" : EntityUtils.encodeBase64Binary(rec.key())) +
                            EntityUtils.encodeBase64Binary(rec.value())
            );
        Set<String> outputSet = new HashSet<String>();
        for(ConsumerRecord rec : consumed)
            outputSet.add((rec.getKey() == null ? "null" : rec.getKeyEncoded()) + rec.getValueEncoded());
        assertEquals(inputSet, outputSet);
    }

    protected void consumeForTimeout(String instanceUri, String topic) {
        long started = System.currentTimeMillis();
        Response response = request(instanceUri + "/topics/" + topic).get();
        long finished = System.currentTimeMillis();
        assertOKResponse(response, Versions.KAFKA_MOST_SPECIFIC_DEFAULT);
        List<ConsumerRecord> consumed =
            response.readEntity(new GenericType<List<ConsumerRecord>>(){});
        assertEquals(0, consumed.size());

        // Note that this is only approximate and really only works if you assume the read call has
        // a dedicated ConsumerWorker thread. Also note that we have to include the consumer
        // request timeout, the iterator timeout used for "peeking", and the backoff period, as well
        // as some extra slack for general overhead (which apparently mostly comes from running the
        // request and can be quite substantial).
        final int TIMEOUT = restConfig.getInt(KafkaRestConfig.CONSUMER_REQUEST_TIMEOUT_MS_CONFIG);
        final int TIMEOUT_SLACK =
            restConfig.getInt(KafkaRestConfig.CONSUMER_ITERATOR_BACKOFF_MS_CONFIG)
            + restConfig.getInt(KafkaRestConfig.CONSUMER_ITERATOR_TIMEOUT_MS_CONFIG) + 150;
        long elapsed = finished - started;
        assertTrue(
            "Consumer request should not return before the timeout when no data is available",
            elapsed > TIMEOUT
        );
        assertTrue(
            "Consumer request should timeout approximately within the request timeout period",
            (elapsed - TIMEOUT) < TIMEOUT_SLACK
        );
    }

    protected void commitOffsets(String instanceUri) {
        Response response = request(instanceUri).post(Entity.entity(null, Versions.KAFKA_MOST_SPECIFIC_DEFAULT));
        assertOKResponse(response, Versions.KAFKA_MOST_SPECIFIC_DEFAULT);
        // We don't verify offsets since they'll depend on how data gets distributed to partitions. Just parse to check
        // the output is formatted validly.
        List<TopicPartitionOffset> offsets = response.readEntity(new GenericType<List<TopicPartitionOffset>>() {});
    }

    // Either topic or instance not found
    protected void consumeForNotFoundError(String instanceUri, String topic) {
        Response response = request(instanceUri + "/topics/" + topic)
                .get();
        assertErrorResponse(Response.Status.NOT_FOUND, response,
                            Errors.CONSUMER_INSTANCE_NOT_FOUND_ERROR_CODE,
                            Errors.CONSUMER_INSTANCE_NOT_FOUND_MESSAGE,
                            Versions.KAFKA_MOST_SPECIFIC_DEFAULT);
    }

    protected void deleteConsumer(String instanceUri) {
        Response response = request(instanceUri).delete();
        assertErrorResponse(Response.Status.NO_CONTENT, response,
                            0, null, Versions.KAFKA_MOST_SPECIFIC_DEFAULT);
    }
}
