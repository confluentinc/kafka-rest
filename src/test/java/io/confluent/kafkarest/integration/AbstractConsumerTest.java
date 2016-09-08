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
package io.confluent.kafkarest.integration;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaJsonSerializer;
import io.confluent.kafkarest.Errors;
import io.confluent.kafkarest.KafkaRestConfig;
import io.confluent.kafkarest.TestUtils;
import io.confluent.kafkarest.Versions;
import io.confluent.kafkarest.entities.AbstractConsumerRecord;
import io.confluent.kafkarest.entities.BinaryConsumerRecord;
import io.confluent.kafkarest.entities.ConsumerInstanceConfig;
import io.confluent.kafkarest.entities.CreateConsumerInstanceResponse;
import io.confluent.kafkarest.entities.EmbeddedFormat;
import io.confluent.kafkarest.entities.TopicPartitionOffset;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import javax.ws.rs.client.Entity;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.Response;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static io.confluent.kafkarest.TestUtils.assertErrorResponse;
import static io.confluent.kafkarest.TestUtils.assertOKResponse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class AbstractConsumerTest extends ClusterTestHarness {

  public AbstractConsumerTest() {
  }

  public AbstractConsumerTest(int numBrokers, boolean withSchemaRegistry) {
    super(numBrokers, withSchemaRegistry);
  }
  protected void produceBinaryMessages(List<ProducerRecord<byte[], byte[]>> records) {
    Properties props = new Properties();
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
    props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
    props.setProperty(ProducerConfig.ACKS_CONFIG, "all");
    Producer<byte[], byte[]> producer = new KafkaProducer<byte[], byte[]>(props);
    for (ProducerRecord<byte[], byte[]> rec : records) {
      try {
        producer.send(rec).get();
      } catch (Exception e) {
        fail("Consumer test couldn't produce input messages to Kafka");
      }
    }
    producer.close();
  }

  protected void produceJsonMessages(List<ProducerRecord<Object, Object>> records) {
    Properties props = new Properties();
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaJsonSerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaJsonSerializer.class);
    props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
    props.setProperty(ProducerConfig.ACKS_CONFIG, "all");
    Producer<Object, Object> producer = new KafkaProducer<Object, Object>(props);
    for (ProducerRecord<Object, Object> rec : records) {
      try {
        producer.send(rec).get();
      } catch (Exception e) {
        fail("Consumer test couldn't produce input messages to Kafka");
      }
    }
    producer.close();
  }

  protected void produceAvroMessages(List<ProducerRecord<Object, Object>> records) {
    HashMap<String, Object> serProps = new HashMap<String, Object>();
    serProps.put("schema.registry.url", schemaRegConnect);
    final KafkaAvroSerializer avroKeySerializer = new KafkaAvroSerializer();
    avroKeySerializer.configure(serProps, true);
    final KafkaAvroSerializer avroValueSerializer = new KafkaAvroSerializer();
    avroValueSerializer.configure(serProps, false);

    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
    props.put(ProducerConfig.ACKS_CONFIG, "all");
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);

    KafkaProducer<Object, Object> producer
        = new KafkaProducer<Object, Object>(props, avroKeySerializer, avroValueSerializer);
    for (ProducerRecord<Object, Object> rec : records) {
      try {
        producer.send(rec).get();
      } catch (Exception e) {
        fail("Consumer test couldn't produce input messages to Kafka");
      }
    }
    producer.close();
  }

  protected Response createConsumerInstance(String groupName, String id,
                                            String name, EmbeddedFormat format) {
    ConsumerInstanceConfig config = null;
    if (id != null || name != null || format != null) {
      config = new ConsumerInstanceConfig(
          id, name, (format != null ? format.toString() : null), null, null);
    }
    return request("/consumers/" + groupName)
        .post(Entity.entity(config, Versions.KAFKA_MOST_SPECIFIC_DEFAULT));
  }

  protected String consumerNameFromInstanceUrl(String url) {
    try {
      String[] pathComponents = new URL(url).getPath().split("/");
      return pathComponents[pathComponents.length-1];
    } catch (MalformedURLException e) {
      throw new RuntimeException(e);
    }
  }

  // Need to start consuming before producing since consumer is instantiated internally and
  // starts at latest offset
  protected String startConsumeMessages(String groupName, String topic, EmbeddedFormat format,
                                        String expectedMediatype) {
    return startConsumeMessages(groupName, topic, format, expectedMediatype, false);
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
   * @param expectFailure     if true, expect the initial read request to generate a 404
   * @return the new consumer instance's base URI
   */
  protected String startConsumeMessages(String groupName, String topic, EmbeddedFormat format,
                                        String expectedMediatype,
                                        boolean expectFailure) {
    Response createResponse = createConsumerInstance(groupName, null, null, format);
    assertOKResponse(createResponse, Versions.KAFKA_MOST_SPECIFIC_DEFAULT);

    CreateConsumerInstanceResponse instanceResponse =
        createResponse.readEntity(CreateConsumerInstanceResponse.class);
    assertNotNull(instanceResponse.getInstanceId());
    assertTrue(instanceResponse.getInstanceId().length() > 0);
    assertTrue("Base URI should contain the consumer instance ID",
               instanceResponse.getBaseUri().contains(instanceResponse.getInstanceId()));

    // Start consuming. Since production hasn't started yet, this is expected to timeout.
    Response response = request(instanceResponse.getBaseUri() + "/topics/" + topic)
        .accept(expectedMediatype).get();
    if (expectFailure) {
      assertErrorResponse(Response.Status.NOT_FOUND, response,
                          Errors.TOPIC_NOT_FOUND_ERROR_CODE,
                          Errors.TOPIC_NOT_FOUND_MESSAGE,
                          expectedMediatype);
    } else {
      assertOKResponse(response, expectedMediatype);
      List<BinaryConsumerRecord> consumed = response.readEntity(
          new GenericType<List<BinaryConsumerRecord>>() {
          });
      assertEquals(0, consumed.size());
    }

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
  protected <KafkaK, KafkaV, ClientK, ClientV, RecordType extends AbstractConsumerRecord<ClientK, ClientV>>
  void assertEqualsMessages(
      List<ProducerRecord<KafkaK, KafkaV>> records, // input messages
      List<RecordType> consumed, // output messages
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
    for (AbstractConsumerRecord<ClientK, ClientV> rec : consumed) {
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

  protected <KafkaK, KafkaV, ClientK, ClientV, RecordType extends AbstractConsumerRecord<ClientK, ClientV>>
  void simpleConsumeMessages(
      String topicName,
      int offset,
      Integer count,
      List<ProducerRecord<KafkaK, KafkaV>> records,
      String accept,
      String responseMediatype,
      GenericType<List<RecordType>> responseEntityType,
      Converter converter) {

    Map<String, String> queryParams = new HashMap<String, String>();
    queryParams.put("offset", Integer.toString(offset));
    if (count != null) {
      queryParams.put("count", count.toString());
    }

    Response response = request("/topics/" + topicName + "/partitions/0/messages", queryParams)
        .accept(accept).get();
    assertOKResponse(response, responseMediatype);
    List<RecordType> consumed = response.readEntity(responseEntityType);
    assertEquals(records.size(), consumed.size());

    assertEqualsMessages(records, consumed, converter);
  }

  protected <KafkaK, KafkaV, ClientK, ClientV, RecordType extends AbstractConsumerRecord<ClientK, ClientV>>
  void consumeMessages(
      String instanceUri, String topic, List<ProducerRecord<KafkaK, KafkaV>> records,
      String accept, String responseMediatype,
      GenericType<List<RecordType>> responseEntityType,
      Converter converter) {
    Response response = request(instanceUri + "/topics/" + topic)
        .accept(accept).get();
    assertOKResponse(response, responseMediatype);
    List<RecordType> consumed = response.readEntity(responseEntityType);
    assertEquals(records.size(), consumed.size());

    assertEqualsMessages(records, consumed, converter);
  }

  protected <K, V, RecordType extends AbstractConsumerRecord<K, V>> void consumeForTimeout(
      String instanceUri, String topic, String accept, String responseMediatype,
      GenericType<List<RecordType>> responseEntityType) {
    long started = System.currentTimeMillis();
    Response response = request(instanceUri + "/topics/" + topic)
        .accept(accept).get();
    long finished = System.currentTimeMillis();
    assertOKResponse(response, responseMediatype);
    List<RecordType> consumed = response.readEntity(responseEntityType);
    assertEquals(0, consumed.size());

    // Note that this is only approximate and really only works if you assume the read call has
    // a dedicated ConsumerWorker thread. Also note that we have to include the consumer
    // request timeout, the iterator timeout used for "peeking", and the backoff period, as well
    // as some extra slack for general overhead (which apparently mostly comes from running the
    // request and can be quite substantial).
    final int TIMEOUT = restConfig.getInt(KafkaRestConfig.CONSUMER_REQUEST_TIMEOUT_MS_CONFIG);
    final int TIMEOUT_SLACK =
        restConfig.getInt(KafkaRestConfig.CONSUMER_ITERATOR_BACKOFF_MS_CONFIG) + 500;
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
        .post(Entity.entity(null, Versions.KAFKA_MOST_SPECIFIC_DEFAULT));
    assertOKResponse(response, Versions.KAFKA_MOST_SPECIFIC_DEFAULT);
    // We don't verify offsets since they'll depend on how data gets distributed to partitions.
    // Just parse to check the output is formatted validly.
    List<TopicPartitionOffset>
        offsets =
        response.readEntity(new GenericType<List<TopicPartitionOffset>>() {
        });
  }

  // Either topic or instance not found
  protected void consumeForNotFoundError(String instanceUri, String topic) {
    Response response = request(instanceUri + "/topics/" + topic)
        .get();
    assertErrorResponse(Response.Status.NOT_FOUND, response,
                        Errors.CONSUMER_INSTANCE_NOT_FOUND_ERROR_CODE,
                        Errors.CONSUMER_INSTANCE_NOT_FOUND_MESSAGE,
                        Versions.KAFKA_V1_JSON_BINARY);
  }

  protected void deleteConsumer(String instanceUri) {
    Response response = request(instanceUri).delete();
    assertErrorResponse(Response.Status.NO_CONTENT, response,
                        0, null, Versions.KAFKA_MOST_SPECIFIC_DEFAULT);
  }
}
