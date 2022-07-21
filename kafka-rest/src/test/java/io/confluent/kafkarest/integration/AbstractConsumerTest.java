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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

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
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.Response;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Assert;

public class AbstractConsumerTest extends ClusterTestHarness {

  public AbstractConsumerTest() {
  }

  public AbstractConsumerTest(int numBrokers, boolean withSchemaRegistry) {
    super(numBrokers, withSchemaRegistry);
  }

  protected Response createConsumerInstance(String groupName, String id,
                                            String name, EmbeddedFormat format) {
    CreateConsumerInstanceRequest config = null;
    if (id != null || name != null || format != null) {
      config =
          new CreateConsumerInstanceRequest(
              id,
              name,
              format != null ? format.toString() : null,
              /* autoOffsetReset= */ null,
              /* autoCommitEnable */ null,
              /* responseMinBytes= */ null,
              /* requestWaitMs= */ null);
    }
    return request("/consumers/" + groupName)
        .post(Entity.entity(config, Versions.KAFKA_V2_JSON));
  }

  protected String consumerNameFromInstanceUrl(String url) {
    try {
      String[] pathComponents = new URL(url).getPath().split("/");
      return pathComponents[pathComponents.length-1];
    } catch (MalformedURLException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Start a new consumer instance and start consuming messages. This expects that you have not
   * produced any data so the initial read request will timeout.
   *
   * @param groupName         consumer group name
   * @param topic             topic to consume
   * @param format            embedded format to use. If null, an null ConsumerInstanceConfig is
   *                          sent, resulting in default settings
   * @param expectedMediatype expected Content-Type of response
   * @return the new consumer instance's base URI
   */
  protected String startConsumeMessages(String groupName, String topic, EmbeddedFormat format,
                                        String expectedMediatype) {
    Response createResponse = createConsumerInstance(groupName, null, null, format);
    assertOKResponse(createResponse, Versions.KAFKA_V2_JSON);

    CreateConsumerInstanceResponse instanceResponse =
            TestUtils.tryReadEntityOrLog(createResponse, CreateConsumerInstanceResponse.class);
    assertNotNull(instanceResponse.getInstanceId());
    assertTrue(instanceResponse.getInstanceId().length() > 0);
    assertTrue("Base URI should contain the consumer instance ID",
               instanceResponse.getBaseUri().contains(instanceResponse.getInstanceId()));

    ConsumerSubscriptionRecord subscribeRequest =
        new ConsumerSubscriptionRecord(Collections.singletonList(topic), /* topicPattern= */ null);
    Response subscribeResponse =
        request(instanceResponse.getBaseUri() + "/subscription")
            .accept(Versions.KAFKA_V2_JSON)
            .post(Entity.entity(subscribeRequest, Versions.KAFKA_V2_JSON));
    Assert.assertEquals(Response.Status.NO_CONTENT.getStatusCode(), subscribeResponse.getStatus());

    // Start consuming. Since production hasn't started yet, this is expected to timeout.
    Response response = request(instanceResponse.getBaseUri() + "/records").accept(expectedMediatype).get();

    assertOKResponse(response, expectedMediatype);
    List<BinaryConsumerRecord> consumed = TestUtils.tryReadEntityOrLog(response,
        new GenericType<List<BinaryConsumerRecord>>() {
        });
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
      Object key = TestUtils.encodeComparable(
          (converter != null ? converter.convert(rec.key()) : rec.key())),
          value = TestUtils.encodeComparable(
              (converter != null ? converter.convert(rec.value()) : rec.value()));
      inputSetCounts.put(key,
                         (inputSetCounts.get(key) == null ? 0 : inputSetCounts.get(key)) + 1);
      inputSetCounts.put(value,
                         (inputSetCounts.get(value) == null ? 0 : inputSetCounts.get(value)) + 1);
    }
    Map<Object, Integer> outputSetCounts = new HashMap<Object, Integer>();
    for (ConsumerRecord<ClientK, ClientV> rec : consumed) {
      Object key = TestUtils.encodeComparable(rec.getKey()),
          value = TestUtils.encodeComparable(rec.getValue());
      outputSetCounts.put(
          key,
          (outputSetCounts.get(key) == null ? 0 : outputSetCounts.get(key)) + 1);
      outputSetCounts.put(
          value,
          (outputSetCounts.get(value) == null ? 0 : outputSetCounts.get(value)) + 1);
    }
    assertEquals(inputSetCounts, outputSetCounts);
  }

  protected <KafkaK, KafkaV, ClientK, ClientV, RecordType>
  void consumeMessages(
      String instanceUri, List<ProducerRecord<KafkaK, KafkaV>> records,
      String accept, String responseMediatype,
      GenericType<List<RecordType>> responseEntityType,
      Converter converter,
      Function<RecordType, ConsumerRecord<ClientK, ClientV>> fromJsonWrapper) {
    Response response = request(instanceUri + "/records").accept(accept).get();
    assertOKResponse(response, responseMediatype);
    List<RecordType> consumed = TestUtils.tryReadEntityOrLog(response, responseEntityType);
    assertEquals(records.size(), consumed.size());

    assertEqualsMessages(
        records, consumed.stream().map(fromJsonWrapper).collect(Collectors.toList()), converter);
  }

  protected <RecordType> void consumeForTimeout(
      String instanceUri, String accept, String responseMediatype,
      GenericType<List<RecordType>> responseEntityType) {
    long started = System.currentTimeMillis();
    Response response = request(instanceUri + "/records")
        .accept(accept).get();
    long finished = System.currentTimeMillis();
    assertOKResponse(response, responseMediatype);
    List<RecordType> consumed = TestUtils.tryReadEntityOrLog(response, responseEntityType);
    assertEquals(0, consumed.size());

    // Note that this is only approximate and really only works if you assume the read call has
    // a dedicated thread. Also note that we have to include the consumer
    // request timeout, the iterator timeout used for "peeking", and the backoff period, as well
    // as some extra slack for general overhead (which apparently mostly comes from running the
    // request and can be quite substantial).
    final int TIMEOUT = restConfig.getInt(KafkaRestConfig.CONSUMER_REQUEST_TIMEOUT_MS_CONFIG);
    final int TIMEOUT_SLACK =
        restConfig.getInt(KafkaRestConfig.CONSUMER_ITERATOR_BACKOFF_MS_CONFIG)
        + restConfig.getInt(KafkaRestConfig.CONSUMER_ITERATOR_TIMEOUT_MS_CONFIG) + 500;
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
    Response response = request(instanceUri + "/offsets/")
        .post(Entity.entity(null, Versions.KAFKA_V2_JSON));
    assertOKResponse(response, Versions.KAFKA_V2_JSON);
    // We don't verify offsets since they'll depend on how data gets distributed to partitions.
    // Just parse to check the output is formatted validly.
    CommitOffsetsResponse offsets =
        TestUtils.tryReadEntityOrLog(response, CommitOffsetsResponse.class);
  }

  // Either topic or instance not found
  protected void consumeForNotFoundError(String instanceUri) {
    Response response = request(instanceUri + "/records").get();
    assertErrorResponse(Response.Status.NOT_FOUND, response,
                        Errors.CONSUMER_INSTANCE_NOT_FOUND_ERROR_CODE,
                        Errors.CONSUMER_INSTANCE_NOT_FOUND_MESSAGE,
                        Versions.KAFKA_V2_JSON_BINARY);
  }

  protected void deleteConsumer(String instanceUri) {
    Response response = request(instanceUri).delete();
    assertErrorResponse(Response.Status.NO_CONTENT, response,
                        0, null, Versions.KAFKA_V2_JSON);
  }
}
