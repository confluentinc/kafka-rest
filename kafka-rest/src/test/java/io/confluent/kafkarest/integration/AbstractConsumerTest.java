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

package io.confluent.kafkarest.integration;

import static io.confluent.kafkarest.TestUtils.assertErrorResponse;
import static io.confluent.kafkarest.TestUtils.assertOKResponse;
import static io.confluent.kafkarest.TestUtils.testWithRetry;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import io.confluent.kafkarest.Errors;
import io.confluent.kafkarest.KafkaRestConfig;
import io.confluent.kafkarest.TestUtils;
import io.confluent.kafkarest.Versions;
import io.confluent.kafkarest.entities.ConsumerRecord;
import io.confluent.kafkarest.entities.EmbeddedFormat;
import io.confluent.kafkarest.entities.v2.BinaryConsumerRecord;
import io.confluent.kafkarest.entities.v2.CommitOffsetsResponse;
import io.confluent.kafkarest.entities.v2.ConsumerSubscriptionRecord;
import io.confluent.kafkarest.entities.v2.CreateConsumerInstanceRequest;
import io.confluent.kafkarest.entities.v2.CreateConsumerInstanceResponse;
import io.confluent.kafkarest.entities.v2.JsonConsumerRecord;
import io.confluent.kafkarest.entities.v2.SchemaConsumerRecord;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.core.GenericType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.Response.Status;
import java.net.MalformedURLException;
import java.net.URL;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AbstractConsumerTest extends ClusterTestHarness {

  private static final long ONE_SECOND_MS = 1000L;
  private static final Logger log = LoggerFactory.getLogger(AbstractConsumerTest.class);

  public AbstractConsumerTest() {}

  public AbstractConsumerTest(int numBrokers, boolean withSchemaRegistry) {
    super(numBrokers, withSchemaRegistry);
  }

  protected Response createConsumerInstance(
      String groupName, String id, String name, EmbeddedFormat format, String autoOffsetReset) {
    CreateConsumerInstanceRequest config = null;
    if (id != null || name != null || format != null) {
      config =
          new CreateConsumerInstanceRequest(
              id,
              name,
              format != null ? format.toString() : null,
              /* autoOffsetReset= */ autoOffsetReset,
              /* autoCommitEnable */ null,
              /* responseMinBytes= */ null,
              /* requestWaitMs= */ null);
    }
    return request("/consumers/" + groupName).post(Entity.entity(config, Versions.KAFKA_V2_JSON));
  }

  protected String consumerNameFromInstanceUrl(String url) {
    try {
      String[] pathComponents = new URL(url).getPath().split("/");
      return pathComponents[pathComponents.length - 1];
    } catch (MalformedURLException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Start a new consumer instance and start consuming messages. This expects that you have not
   * produced any data so the initial read request will timeout.
   *
   * @param groupName consumer group name
   * @param topic topic to consume
   * @param format embedded format to use. If null, an null ConsumerInstanceConfig is sent,
   *     resulting in default settings
   * @param expectedMediatype expected Content-Type of response
   * @return the new consumer instance's base URI
   */
  protected String startConsumeMessages(
      String groupName,
      String topic,
      EmbeddedFormat format,
      String expectedMediatype,
      String autoOffsetReset) {
    return startConsumeMessages(
        groupName, Collections.singletonList(topic), format, expectedMediatype, autoOffsetReset);
  }

  protected String startConsumeMessages(
      String groupName,
      List<String> topics,
      EmbeddedFormat format,
      String expectedMediatype,
      String autoOffsetReset) {
    Response createResponse = null;
    try {
      createResponse = createConsumerInstance(groupName, null, null, format, autoOffsetReset);
      assertOKResponse(createResponse, Versions.KAFKA_V2_JSON);
    } catch (AssertionError e) {
      if (createResponse != null
          && createResponse.getStatus() == Response.Status.NOT_FOUND.getStatusCode()) {
        pause();
        createResponse = createConsumerInstance(groupName, null, null, format, autoOffsetReset);
        assertOKResponse(createResponse, Versions.KAFKA_V2_JSON);
      } else {
        fail("Create consumer instance failed.", e);
      }
    }

    CreateConsumerInstanceResponse instanceResponse =
        TestUtils.tryReadEntityOrLog(createResponse, CreateConsumerInstanceResponse.class);
    assertNotNull(instanceResponse.getInstanceId());
    assertTrue(instanceResponse.getInstanceId().length() > 0);
    assertTrue(
        instanceResponse.getBaseUri().contains(instanceResponse.getInstanceId()),
        "Base URI should contain the consumer instance ID");

    ConsumerSubscriptionRecord subscribeRequest =
        new ConsumerSubscriptionRecord(topics, /* topicPattern= */ null);
    Response subscribeResponse =
        request(instanceResponse.getBaseUri() + "/subscription")
            .accept(Versions.KAFKA_V2_JSON)
            .post(Entity.entity(subscribeRequest, Versions.KAFKA_V2_JSON));
    assertEquals(Response.Status.NO_CONTENT.getStatusCode(), subscribeResponse.getStatus());

    Response response = null;
    // Start consuming. Since production hasn't started yet, this is expected to timeout.
    try {
      response =
          request(instanceResponse.getBaseUri() + "/records").accept(expectedMediatype).get();
      assertOKResponse(response, expectedMediatype);
    } catch (AssertionError e) {
      if (response != null && response.getStatus() == Response.Status.NOT_FOUND.getStatusCode()) {
        pause();
        response =
            request(instanceResponse.getBaseUri() + "/records").accept(expectedMediatype).get();
        assertOKResponse(response, expectedMediatype);
      } else {
        fail("Consume of records failed", e);
      }
    }
    List<BinaryConsumerRecord> consumed =
        TestUtils.tryReadEntityOrLog(response, new GenericType<List<BinaryConsumerRecord>>() {});
    assertEquals(0, consumed.size());
    return instanceResponse.getBaseUri();
  }

  // Interface for converter from type used by Kafka producer (e.g. GenericRecord) to the type
  // actually consumed (e.g. JsonNode) so we can get both input and output in consistent form to
  // generate comparable data sets for validation
  protected static interface Converter {

    public Object convert(Object obj);
  }

  // This requires a lot of type info because we use the raw ProducerRecords used to work with
  // the Kafka producer directly (e.g. Object for GenericRecord+primitive for Avro) and the
  // consumed data type on the receiver (JsonNode, since the data has been converted to Json).
  protected <KafkaK, KafkaV, ClientK, ClientV> void assertEqualsMessages(
      List<ProducerRecord<KafkaK, KafkaV>> records, // input messages
      List<ConsumerRecord<ClientK, ClientV>> consumed, // output messages
      Converter converter) {

    // Since this is used for unkeyed messages, this can't rely on ordering of messages
    Map<Object, Integer> inputSetCounts = new HashMap<Object, Integer>();
    for (ProducerRecord<KafkaK, KafkaV> rec : records) {
      Object key =
          TestUtils.encodeComparable(
              (converter != null ? converter.convert(rec.key()) : rec.key()));
      Object value =
          TestUtils.encodeComparable(
              (converter != null ? converter.convert(rec.value()) : rec.value()));
      inputSetCounts.put(key, (inputSetCounts.get(key) == null ? 0 : inputSetCounts.get(key)) + 1);
      inputSetCounts.put(
          value, (inputSetCounts.get(value) == null ? 0 : inputSetCounts.get(value)) + 1);
    }
    Map<Object, Integer> outputSetCounts = new HashMap<Object, Integer>();
    for (ConsumerRecord<ClientK, ClientV> rec : consumed) {
      Object key = TestUtils.encodeComparable(rec.getKey());
      Object value = TestUtils.encodeComparable(rec.getValue());
      outputSetCounts.put(
          key, (outputSetCounts.get(key) == null ? 0 : outputSetCounts.get(key)) + 1);
      outputSetCounts.put(
          value, (outputSetCounts.get(value) == null ? 0 : outputSetCounts.get(value)) + 1);
    }
    assertEquals(inputSetCounts, outputSetCounts);
  }

  protected <KafkaK, KafkaV, ClientK, ClientV, RecordType> void consumeMessages(
      String instanceUri,
      List<ProducerRecord<KafkaK, KafkaV>> records,
      String accept,
      String responseMediatype,
      GenericType<List<RecordType>> responseEntityType,
      Converter converter,
      Function<RecordType, ConsumerRecord<ClientK, ClientV>> fromJsonWrapper) {

    List<RecordType> consumed = new ArrayList<>();

    testWithRetry(
        () -> {
          Response response = request(instanceUri + "/records").accept(accept).get();
          assertOKResponse(response, responseMediatype);
          consumed.addAll(TestUtils.tryReadEntityOrLog(response, responseEntityType));
          try {
            assertEquals(records.size(), consumed.size());
          } catch (AssertionError assertionError) {
            logConsumeData(records, consumed);
            throw assertionError;
          }
        });

    assertEqualsMessages(
        records, consumed.stream().map(fromJsonWrapper).collect(Collectors.toList()), converter);
  }

  private final <KafkaK, KafkaV, RecordType> void logConsumeData(
      List<ProducerRecord<KafkaK, KafkaV>> records, List<RecordType> consumed) {
    log.info("Records produced : {}", records.size());
    log.info("Consume failed.  Record offsets are:");
    consumed.forEach(
        record -> {
          if (record instanceof SchemaConsumerRecord) {
            log.info("{}", ((SchemaConsumerRecord) record).getOffset());
          } else if (record instanceof BinaryConsumerRecord) {
            log.info("{}", ((BinaryConsumerRecord) record).getOffset());
          } else if (record instanceof JsonConsumerRecord) {
            log.info("{}", ((JsonConsumerRecord) record).getOffset());
          } else {
            log.info("Unknown record type {}", record.toString());
          }
        });
  }

  private final void pause() {
    pause(1);
  }

  private final void pause(int times) {
    try {
      Thread.sleep(times * ONE_SECOND_MS);
    } catch (InterruptedException e) {
      // Noop
    }
  }

  protected <RecordType> void consumeForTimeout(
      String instanceUri,
      String accept,
      String responseMediatype,
      GenericType<List<RecordType>> responseEntityType) {
    long started = System.currentTimeMillis();
    Response response = request(instanceUri + "/records").accept(accept).get();
    long finished = System.currentTimeMillis();
    assertOKResponse(response, responseMediatype);
    List<RecordType> consumed = TestUtils.tryReadEntityOrLog(response, responseEntityType);
    assertEquals(0, consumed.size());

    // Note that this is only approximate and really only works if you assume the read call has
    // a dedicated thread. Also note that we have to include the consumer
    // request timeout, the iterator timeout used for "peeking", and the backoff period, as well
    // as some extra slack for general overhead (which apparently mostly comes from running the
    // request and can be quite substantial).
    final int timeout = restConfig.getInt(KafkaRestConfig.CONSUMER_REQUEST_TIMEOUT_MS_CONFIG);
    final long timeoutSlack =
        restConfig.getInt(KafkaRestConfig.CONSUMER_ITERATOR_BACKOFF_MS_CONFIG)
            + restConfig.getInt(KafkaRestConfig.CONSUMER_ITERATOR_TIMEOUT_MS_CONFIG)
            + ONE_SECOND_MS; // This test is inherently flakey, and probably needs a mocked back
    // end, but for now we can give it lots of slack
    long elapsed = finished - started;
    assertTrue(
        elapsed > timeout,
        "Consumer request should not return before the timeout when no data is available");
    assertTrue(
        (elapsed - timeout) < timeoutSlack,
        "Consumer request should timeout approximately within the request timeout period");
  }

  protected void seekToTimestamp(
      String instanceUri, String topicName, int partitionId, Instant timestamp) {
    Response response =
        request(instanceUri + "/positions")
            .post(
                Entity.entity(
                    String.format(
                        "{\"timestamps\":"
                            + "[{\"topic\": \"%s\", \"partition\": %d, \"timestamp\": \"%s\"}]}",
                        topicName, partitionId, DateTimeFormatter.ISO_INSTANT.format(timestamp)),
                    Versions.KAFKA_V2_JSON));

    assertEquals(Status.NO_CONTENT.getStatusCode(), response.getStatus());
  }

  protected void commitOffsets(String instanceUri) {
    Response response =
        request(instanceUri + "/offsets/").post(Entity.entity(null, Versions.KAFKA_V2_JSON));
    assertOKResponse(response, Versions.KAFKA_V2_JSON);
    // We don't verify offsets since they'll depend on how data gets distributed to partitions.
    // Just parse to check the output is formatted validly.
    CommitOffsetsResponse offsets =
        TestUtils.tryReadEntityOrLog(response, CommitOffsetsResponse.class);
  }

  // Either topic or instance not found
  protected void consumeForNotFoundError(String instanceUri) {
    Response response = request(instanceUri + "/records").get();
    assertErrorResponse(
        Response.Status.NOT_FOUND,
        response,
        Errors.CONSUMER_INSTANCE_NOT_FOUND_ERROR_CODE,
        Errors.CONSUMER_INSTANCE_NOT_FOUND_MESSAGE,
        Versions.KAFKA_V2_JSON_BINARY);
  }

  protected void deleteConsumer(String instanceUri) {
    Response response = request(instanceUri).delete();
    assertErrorResponse(Response.Status.NO_CONTENT, response, 0, null, Versions.KAFKA_V2_JSON);
  }
}
