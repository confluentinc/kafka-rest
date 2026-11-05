/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.kafkarest.integration.v2;

import static io.confluent.kafkarest.TestUtils.TEST_WITH_PARAMETERIZED_QUORUM_NAME;
import static io.confluent.kafkarest.TestUtils.assertOKResponse;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafkarest.TestUtils;
import io.confluent.kafkarest.Versions;
import io.confluent.kafkarest.converters.JsonSchemaConverter;
import io.confluent.kafkarest.entities.EmbeddedFormat;
import io.confluent.kafkarest.entities.v2.PartitionOffset;
import io.confluent.kafkarest.entities.v2.ProduceResponse;
import io.confluent.kafkarest.entities.v2.SchemaTopicProduceRequest;
import io.confluent.kafkarest.entities.v2.SchemaTopicProduceRequest.SchemaTopicProduceRecord;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.core.Response;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public final class JsonSchemaProduceConsumeTest extends SchemaProduceConsumeTest {

  private static final JsonSchemaConverter JSON_SCHEMA_CONVERTER = new JsonSchemaConverter();

  private static final JsonSchema KEY_SCHEMA = new JsonSchema("{\"type\":\"number\"}");

  private static final JsonSchema VALUE_SCHEMA =
      new JsonSchema("{\"type\":\"object\",\"properties\":{\"value\":{\"type\":\"number\"}}}");

  private static final List<SchemaTopicProduceRecord> PRODUCE_RECORDS =
      Arrays.asList(
          new SchemaTopicProduceRecord(
              new IntNode(1),
              JSON_SCHEMA_CONVERTER.toJson(getMessage(11)).getJson(),
              /* partition= */ 0),
          new SchemaTopicProduceRecord(
              new IntNode(2),
              JSON_SCHEMA_CONVERTER.toJson(getMessage(12)).getJson(),
              /* partition= */ 0),
          new SchemaTopicProduceRecord(
              new IntNode(3),
              JSON_SCHEMA_CONVERTER.toJson(getMessage(13)).getJson(),
              /* partition= */ 0));

  private static JsonNode getMessage(int value) {
    ObjectMapper objectMapper = new ObjectMapper();
    ObjectNode root = objectMapper.createObjectNode();
    root.put("value", value);
    return root;
  }

  @Override
  protected EmbeddedFormat getFormat() {
    return EmbeddedFormat.JSONSCHEMA;
  }

  @Override
  protected String getContentType() {
    return Versions.KAFKA_V2_JSON_JSON_SCHEMA;
  }

  @Override
  protected ParsedSchema getKeySchema() {
    return KEY_SCHEMA;
  }

  @Override
  protected ParsedSchema getValueSchema() {
    return VALUE_SCHEMA;
  }

  @Override
  protected List<SchemaTopicProduceRecord> getProduceRecords() {
    return PRODUCE_RECORDS;
  }

  // A null key/value whose JSON Schema accepts null must be published as a Kafka null (no bytes),
  // not as an encoded JSON null. Read the record back as raw bytes to tell the two apart.
  @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
  @ValueSource(strings = {"kraft"})
  public void produceNullWithNullableSchema_publishesKafkaNull(String quorum) {
    String topic = "topic-nullable";
    createTopic(topic, /* numPartitions= */ 1, /* replicationFactor= */ (short) 1);
    SchemaTopicProduceRequest request =
        SchemaTopicProduceRequest.create(
            singletonList(
                new SchemaTopicProduceRecord(
                    NullNode.getInstance(), NullNode.getInstance(), /* partition= */ 0)),
            /* keySchema= */ "{\"type\": \"null\"}",
            /* keySchemaId= */ null,
            /* valueSchema= */ "{\"type\": [\"null\", \"object\"]}",
            /* valueSchemaId= */ null);

    Response response = request("/topics/" + topic).post(Entity.entity(request, getContentType()));
    assertOKResponse(response, Versions.KAFKA_V2_JSON);
    ProduceResponse produceResponse = TestUtils.tryReadEntityOrLog(response, ProduceResponse.class);
    TestUtils.assertPartitionOffsetsEqual(
        singletonList(new PartitionOffset(0, 0L, null, null)), produceResponse.getOffsets());

    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, plaintextBrokerList);
    try (KafkaConsumer<byte[], byte[]> consumer =
        new KafkaConsumer<>(props, new ByteArrayDeserializer(), new ByteArrayDeserializer())) {
      TopicPartition tp = new TopicPartition(topic, 0);
      consumer.assign(singletonList(tp));
      consumer.seekToBeginning(singletonList(tp));
      List<ConsumerRecord<byte[], byte[]>> records = new ArrayList<>();
      long deadline = System.currentTimeMillis() + 20_000;
      while (records.isEmpty() && System.currentTimeMillis() < deadline) {
        consumer.poll(Duration.ofMillis(100)).forEach(records::add);
      }
      assertEquals(1, records.size());
      assertNull(records.get(0).key(), "key must be a Kafka null, not an encoded null");
      assertNull(records.get(0).value(), "value must be a Kafka null (a tombstone)");
    }
  }
}
