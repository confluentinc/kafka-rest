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

import io.confluent.kafkarest.Config;
import io.confluent.kafkarest.ConsumerManager;
import io.confluent.kafkarest.entities.ConsumerRecord;
import io.confluent.kafkarest.entities.CreateConsumerInstanceResponse;
import io.confluent.kafkarest.entities.EntityUtils;
import io.confluent.kafkarest.resources.ConsumersResource;
import io.confluent.kafkarest.resources.TopicsResource;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.ws.rs.client.Entity;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import static io.confluent.kafkarest.TestUtils.assertErrorResponse;
import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

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
                .post(Entity.entity(null, MediaType.APPLICATION_JSON_TYPE), CreateConsumerInstanceResponse.class);
        assertNotNull(instanceResponse.getInstanceId());
        assertTrue(instanceResponse.getInstanceId().length() > 0);
        assertTrue("Base URI should contain the consumer instance ID",
                instanceResponse.getBaseUri().contains(instanceResponse.getInstanceId()));

        // Start consuming. Since production hasn't started yet, this is expected to timeout.
        Response response = request(instanceResponse.getBaseUri() + "/topics/" + topic).get();
        if (expectFailure) {
            assertErrorResponse(Response.Status.NOT_FOUND, response, TopicsResource.MESSAGE_TOPIC_NOT_FOUND);
        } else {
            List<ConsumerRecord> consumed = response.readEntity(new GenericType<List<ConsumerRecord>>() {});
            assertEquals(0, consumed.size());
        }

        return instanceResponse.getBaseUri();
    }

    protected void consumeMessages(String instanceUri, String topic, List<ProducerRecord> records) {
        List<ConsumerRecord> consumed = request(instanceUri + "/topics/" + topic)
                .get(new GenericType<List<ConsumerRecord>>() {
                });
        assertEquals(records.size(), consumed.size());

        // Since this is used for unkeyed messages, this can't rely on ordering of messages
        Set<String> inputSet = new HashSet<>();
        for(ProducerRecord rec : records)
            inputSet.add(
                    (rec.key() == null ? "null" : EntityUtils.encodeBase64Binary(rec.key())) +
                            EntityUtils.encodeBase64Binary(rec.value())
            );
        Set<String> outputSet = new HashSet<>();
        for(ConsumerRecord rec : consumed)
            outputSet.add((rec.getKey() == null ? "null" : rec.getKeyEncoded()) + rec.getValueEncoded());
        assertEquals(inputSet, outputSet);
    }

    protected void consumeForTimeout(String instanceUri, String topic) {
        long started = System.currentTimeMillis();
        List<ConsumerRecord> consumed = request(instanceUri + "/topics/" + topic)
                .get(new GenericType<List<ConsumerRecord>>(){});
        long finished = System.currentTimeMillis();
        assertEquals(0, consumed.size());

        final int TIMEOUT = restConfig.consumerRequestTimeoutMs;
        final int TIMEOUT_SLACK = restConfig.consumerIteratorTimeoutMs + 50;
        assertTrue("Consumer request should not return before the timeout when no data is available", (finished-started) > TIMEOUT);
        assertTrue("Consumer request should timeout approximately within the ", ((finished-started) - TIMEOUT) < TIMEOUT_SLACK);
    }

    // Either topic or instance not found
    protected void consumeForNotFoundError(String instanceUri, String topic) {
        Response response = request(instanceUri + "/topics/" + topic)
                .get();
        assertErrorResponse(Response.Status.NOT_FOUND, response, ConsumerManager.MESSAGE_CONSUMER_INSTANCE_NOT_FOUND);
    }

    protected void deleteConsumer(String instanceUri) {
        Response response = request(instanceUri).delete();
        assertErrorResponse(Response.Status.NO_CONTENT, response, null);
    }
}
