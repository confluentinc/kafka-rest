/*
 * Copyright 2023 Confluent Inc.
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

package io.confluent.kafkarest.integration.v3;

import static io.confluent.kafkarest.TestUtils.TEST_WITH_PARAMETERIZED_QUORUM_NAME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.BinaryNode;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.serializers.KafkaJsonDeserializer;
import io.confluent.kafka.serializers.subject.RecordNameStrategy;
import io.confluent.kafka.serializers.subject.TopicNameStrategy;
import io.confluent.kafkarest.KafkaRestConfig;
import io.confluent.kafkarest.entities.EmbeddedFormat;
import io.confluent.kafkarest.entities.v3.ProduceBatchRequest;
import io.confluent.kafkarest.entities.v3.ProduceBatchRequestEntry;
import io.confluent.kafkarest.entities.v3.ProduceBatchResponse;
import io.confluent.kafkarest.entities.v3.ProduceBatchResponseFailureEntry;
import io.confluent.kafkarest.entities.v3.ProduceBatchResponseSuccessEntry;
import io.confluent.kafkarest.entities.v3.ProduceRequest.EnumSubjectNameStrategy;
import io.confluent.kafkarest.entities.v3.ProduceRequest.ProduceRequestData;
import io.confluent.kafkarest.entities.v3.ProduceRequest.ProduceRequestHeader;
import io.confluent.kafkarest.exceptions.v3.ErrorResponse;
import io.confluent.kafkarest.testing.DefaultKafkaRestTestEnvironment;
import io.confluent.kafkarest.testing.SchemaRegistryFixture.SchemaKey;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

// Temporarily disable to permit investigation without breaking the build
@Tag("IntegrationTest")
@Disabled
public class ProduceBatchActionIntegrationTest {

  private static final String TOPIC_NAME = "topic-1";
  private static final String DEFAULT_KEY_SUBJECT = "topic-1-key";
  private static final String DEFAULT_VALUE_SUBJECT = "topic-1-value";

  @RegisterExtension
  public final DefaultKafkaRestTestEnvironment testEnv = new DefaultKafkaRestTestEnvironment(false);

  @BeforeEach
  public void setUp(TestInfo testInfo) throws Exception {
    Properties restConfigs = new Properties();
    // Adding custom KafkaRestConfigs for individual test-cases/test-methods below.
    if (testInfo.getDisplayName().contains("CallerIsRateLimited")) {
      restConfigs.put(KafkaRestConfig.RATE_LIMIT_ENABLE_CONFIG, "true");
      restConfigs.put(KafkaRestConfig.PRODUCE_RATE_LIMIT_ENABLED, "true");
      restConfigs.put(KafkaRestConfig.RATE_LIMIT_BACKEND_CONFIG, "resilience4j");
      // The happy-path testing, i.e. rest calls below threshold succeed are already covered by the
      // other existing tests. The 4 tests below, 1 per rate-limit config, set a very low rate-limit
      // of "1", to deterministically make sure limits apply and rest-calls see 429s.
      if (testInfo
          .getDisplayName()
          .contains("test_whenGlobalByteLimitReached_thenCallerIsRateLimited")) {

        restConfigs.put(KafkaRestConfig.PRODUCE_MAX_BYTES_GLOBAL_PER_SECOND, "1");
      }
      if (testInfo
          .getDisplayName()
          .contains("test_whenClusterByteLimitReached_thenCallerIsRateLimited")) {

        restConfigs.put(KafkaRestConfig.PRODUCE_MAX_BYTES_PER_SECOND, "1");
      }
      if (testInfo
          .getDisplayName()
          .contains("test_whenGlobalRequestCountLimitReached_thenCallerIsRateLimited")) {

        restConfigs.put(KafkaRestConfig.PRODUCE_MAX_REQUESTS_GLOBAL_PER_SECOND, "1");
      }
      if (testInfo
          .getDisplayName()
          .contains("test_whenClusterRequestCountLimitReached_thenCallerIsRateLimited")) {

        restConfigs.put(KafkaRestConfig.PRODUCE_MAX_REQUESTS_PER_SECOND, "1");
      }
    }
    testEnv.kafkaRest().startApp(restConfigs);

    testEnv.kafkaCluster().createTopic(TOPIC_NAME, 3, (short) 1);
  }

  @AfterEach
  public void tearDown() {
    testEnv.kafkaRest().closeApp();
  }

  @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
  @ValueSource(strings = {"kraft", "zk"})
  public void produceBinary(String quorum) throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    ByteString key = ByteString.copyFromUtf8("foo");
    ByteString value = ByteString.copyFromUtf8("bar");
    ProduceBatchRequest request =
        ProduceBatchRequest.builder()
            .setEntries(
                Collections.singletonList(
                    ProduceBatchRequestEntry.builder()
                        .setId(new TextNode("1"))
                        .setKey(
                            ProduceRequestData.builder()
                                .setFormat(EmbeddedFormat.BINARY)
                                .setData(BinaryNode.valueOf(key.toByteArray()))
                                .build())
                        .setValue(
                            ProduceRequestData.builder()
                                .setFormat(EmbeddedFormat.BINARY)
                                .setData(BinaryNode.valueOf(value.toByteArray()))
                                .build())
                        .setOriginalSize(0L)
                        .build()))
            .build();

    Response response =
        testEnv
            .kafkaRest()
            .target()
            .path("/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME + "/records:batch")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(request, MediaType.APPLICATION_JSON));
    assertEquals(207, response.getStatus());

    ProduceBatchResponseSuccessEntry actual = readResponseSuccessEntry(response, "1");
    ConsumerRecord<byte[], byte[]> produced =
        testEnv
            .kafkaCluster()
            .getRecord(
                TOPIC_NAME,
                actual.getPartitionId(),
                actual.getOffset(),
                new ByteArrayDeserializer(),
                new ByteArrayDeserializer());
    assertEquals(key, ByteString.copyFrom(produced.key()));
    assertEquals(value, ByteString.copyFrom(produced.value()));
  }

  @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
  @ValueSource(strings = {"kraft", "zk"})
  public void produceBinaryWithNullData(String quorum) throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    ProduceBatchRequest request =
        ProduceBatchRequest.builder()
            .setEntries(
                Collections.singletonList(
                    ProduceBatchRequestEntry.builder()
                        .setId(new TextNode("1"))
                        .setKey(
                            ProduceRequestData.builder()
                                .setFormat(EmbeddedFormat.BINARY)
                                .setData(NullNode.getInstance())
                                .build())
                        .setValue(
                            ProduceRequestData.builder()
                                .setFormat(EmbeddedFormat.BINARY)
                                .setData(NullNode.getInstance())
                                .build())
                        .setOriginalSize(0L)
                        .build()))
            .build();

    Response response =
        testEnv
            .kafkaRest()
            .target()
            .path("/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME + "/records:batch")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(request, MediaType.APPLICATION_JSON));
    assertEquals(207, response.getStatus());

    ProduceBatchResponseSuccessEntry actual = readResponseSuccessEntry(response, "1");
    ConsumerRecord<byte[], byte[]> produced =
        testEnv
            .kafkaCluster()
            .getRecord(
                TOPIC_NAME,
                actual.getPartitionId(),
                actual.getOffset(),
                new ByteArrayDeserializer(),
                new ByteArrayDeserializer());
    assertNull(produced.key());
    assertNull(produced.value());
  }

  @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
  @ValueSource(strings = {"kraft", "zk"})
  public void produceBinaryWithInvalidData_throwsBadRequest(String quorum) throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    ProduceBatchRequest request =
        ProduceBatchRequest.builder()
            .setEntries(
                Collections.singletonList(
                    ProduceBatchRequestEntry.builder()
                        .setId(new TextNode("1"))
                        .setKey(
                            ProduceRequestData.builder()
                                .setFormat(EmbeddedFormat.BINARY)
                                .setData(IntNode.valueOf(1))
                                .build())
                        .setValue(
                            ProduceRequestData.builder()
                                .setFormat(EmbeddedFormat.BINARY)
                                .setData(TextNode.valueOf("fooba")) // invalid base64 string
                                .build())
                        .setOriginalSize(0L)
                        .build()))
            .build();

    Response response =
        testEnv
            .kafkaRest()
            .target()
            .path("/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME + "/records:batch")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(request, MediaType.APPLICATION_JSON));
    assertEquals(207, response.getStatus());

    ProduceBatchResponseFailureEntry actual = readResponseFailureEntry(response, "1");
    assertEquals(400, actual.getErrorCode());
  }

  @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
  @ValueSource(strings = {"kraft", "zk"})
  public void produceString(String quorum) throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    String key = "foo";
    String value = "bar";
    ProduceBatchRequest request =
        ProduceBatchRequest.builder()
            .setEntries(
                Collections.singletonList(
                    ProduceBatchRequestEntry.builder()
                        .setId(new TextNode("1"))
                        .setKey(
                            ProduceRequestData.builder()
                                .setFormat(EmbeddedFormat.STRING)
                                .setData(TextNode.valueOf(key))
                                .build())
                        .setValue(
                            ProduceRequestData.builder()
                                .setFormat(EmbeddedFormat.STRING)
                                .setData(TextNode.valueOf(value))
                                .build())
                        .setOriginalSize(0L)
                        .build()))
            .build();

    Response response =
        testEnv
            .kafkaRest()
            .target()
            .path("/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME + "/records:batch")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(request, MediaType.APPLICATION_JSON));
    assertEquals(207, response.getStatus());

    ProduceBatchResponseSuccessEntry actual = readResponseSuccessEntry(response, "1");
    ConsumerRecord<String, String> produced =
        testEnv
            .kafkaCluster()
            .getRecord(
                TOPIC_NAME,
                actual.getPartitionId(),
                actual.getOffset(),
                new StringDeserializer(),
                new StringDeserializer());
    assertEquals(key, produced.key());
    assertEquals(value, produced.value());
  }

  @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
  @ValueSource(strings = {"kraft", "zk"})
  public void produceStringWithEmptyData(String quorum) throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    ProduceBatchRequest request =
        ProduceBatchRequest.builder()
            .setEntries(
                Collections.singletonList(
                    ProduceBatchRequestEntry.builder()
                        .setId(new TextNode("1"))
                        .setKey(
                            ProduceRequestData.builder()
                                .setFormat(EmbeddedFormat.STRING)
                                .setData(TextNode.valueOf(""))
                                .build())
                        .setValue(
                            ProduceRequestData.builder()
                                .setFormat(EmbeddedFormat.STRING)
                                .setData(TextNode.valueOf(""))
                                .build())
                        .setOriginalSize(0L)
                        .build()))
            .build();

    Response response =
        testEnv
            .kafkaRest()
            .target()
            .path("/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME + "/records:batch")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(request, MediaType.APPLICATION_JSON));
    assertEquals(207, response.getStatus());

    ProduceBatchResponseSuccessEntry actual = readResponseSuccessEntry(response, "1");
    ConsumerRecord<String, String> produced =
        testEnv
            .kafkaCluster()
            .getRecord(
                TOPIC_NAME,
                actual.getPartitionId(),
                actual.getOffset(),
                new StringDeserializer(),
                new StringDeserializer());
    assertTrue(produced.key().isEmpty());
    assertTrue(produced.value().isEmpty());
  }

  @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
  @ValueSource(strings = {"kraft", "zk"})
  public void produceStringWithNullData(String quorum) throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    ProduceBatchRequest request =
        ProduceBatchRequest.builder()
            .setEntries(
                Collections.singletonList(
                    ProduceBatchRequestEntry.builder()
                        .setId(new TextNode("1"))
                        .setKey(
                            ProduceRequestData.builder()
                                .setFormat(EmbeddedFormat.STRING)
                                .setData(NullNode.getInstance())
                                .build())
                        .setValue(
                            ProduceRequestData.builder()
                                .setFormat(EmbeddedFormat.STRING)
                                .setData(NullNode.getInstance())
                                .build())
                        .setOriginalSize(0L)
                        .build()))
            .build();

    Response response =
        testEnv
            .kafkaRest()
            .target()
            .path("/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME + "/records:batch")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(request, MediaType.APPLICATION_JSON));
    assertEquals(207, response.getStatus());

    ProduceBatchResponseSuccessEntry actual = readResponseSuccessEntry(response, "1");
    ConsumerRecord<String, String> produced =
        testEnv
            .kafkaCluster()
            .getRecord(
                TOPIC_NAME,
                actual.getPartitionId(),
                actual.getOffset(),
                new StringDeserializer(),
                new StringDeserializer());
    assertNull(produced.key());
    assertNull(produced.value());
  }

  @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
  @ValueSource(strings = {"kraft", "zk"})
  public void produceWithInvalidData_throwsBadRequest(String quorum) throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    String request = "{ \"records\": {\"subject\": \"foobar\" } }";

    Response response =
        testEnv
            .kafkaRest()
            .target()
            .path("/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME + "/records:batch")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(request, MediaType.APPLICATION_JSON));
    assertEquals(422, response.getStatus());

    ErrorResponse actual = response.readEntity(ErrorResponse.class);
    assertEquals(422, actual.getErrorCode());
    assertEquals("Unrecognized field: records", actual.getMessage());
  }

  @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
  @ValueSource(strings = {"kraft", "zk"})
  public void produceJson(String quorum) throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    String key = "foo";
    String value = "bar";
    ProduceBatchRequest request =
        ProduceBatchRequest.builder()
            .setEntries(
                Collections.singletonList(
                    ProduceBatchRequestEntry.builder()
                        .setId(new TextNode("1"))
                        .setKey(
                            ProduceRequestData.builder()
                                .setFormat(EmbeddedFormat.JSON)
                                .setData(TextNode.valueOf(key))
                                .build())
                        .setValue(
                            ProduceRequestData.builder()
                                .setFormat(EmbeddedFormat.JSON)
                                .setData(TextNode.valueOf(value))
                                .build())
                        .setOriginalSize(0L)
                        .build()))
            .build();

    Response response =
        testEnv
            .kafkaRest()
            .target()
            .path("/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME + "/records:batch")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(request, MediaType.APPLICATION_JSON));
    assertEquals(207, response.getStatus());

    ProduceBatchResponseSuccessEntry actual = readResponseSuccessEntry(response, "1");
    KafkaJsonDeserializer<Object> deserializer = new KafkaJsonDeserializer<>();
    deserializer.configure(Collections.emptyMap(), /* isKey= */ false);
    ConsumerRecord<Object, Object> produced =
        testEnv
            .kafkaCluster()
            .getRecord(
                TOPIC_NAME,
                actual.getPartitionId(),
                actual.getOffset(),
                deserializer,
                deserializer);
    assertEquals(key, produced.key());
    assertEquals(value, produced.value());
  }

  @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
  @ValueSource(strings = {"kraft", "zk"})
  public void produceJsonWithNullData(String quorum) throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    ProduceBatchRequest request =
        ProduceBatchRequest.builder()
            .setEntries(
                Collections.singletonList(
                    ProduceBatchRequestEntry.builder()
                        .setId(new TextNode("1"))
                        .setKey(
                            ProduceRequestData.builder()
                                .setFormat(EmbeddedFormat.JSON)
                                .setData(NullNode.getInstance())
                                .build())
                        .setValue(
                            ProduceRequestData.builder()
                                .setFormat(EmbeddedFormat.JSON)
                                .setData(NullNode.getInstance())
                                .build())
                        .setOriginalSize(0L)
                        .build()))
            .build();

    Response response =
        testEnv
            .kafkaRest()
            .target()
            .path("/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME + "/records:batch")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(request, MediaType.APPLICATION_JSON));
    assertEquals(207, response.getStatus());

    ProduceBatchResponseSuccessEntry actual = readResponseSuccessEntry(response, "1");
    KafkaJsonDeserializer<Object> deserializer = new KafkaJsonDeserializer<>();
    deserializer.configure(Collections.emptyMap(), /* isKey= */ false);
    ConsumerRecord<Object, Object> produced =
        testEnv
            .kafkaCluster()
            .getRecord(
                TOPIC_NAME,
                actual.getPartitionId(),
                actual.getOffset(),
                deserializer,
                deserializer);
    assertNull(produced.key());
    assertNull(produced.value());
  }

  @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
  @ValueSource(strings = {"kraft", "zk"})
  public void produceAvroWithRawSchema(String quorum) throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    String key = "foo";
    String value = "bar";
    ProduceBatchRequest request =
        ProduceBatchRequest.builder()
            .setEntries(
                Collections.singletonList(
                    ProduceBatchRequestEntry.builder()
                        .setId(new TextNode("1"))
                        .setKey(
                            ProduceRequestData.builder()
                                .setFormat(EmbeddedFormat.AVRO)
                                .setRawSchema("{\"type\": \"string\"}")
                                .setData(TextNode.valueOf(key))
                                .build())
                        .setValue(
                            ProduceRequestData.builder()
                                .setFormat(EmbeddedFormat.AVRO)
                                .setRawSchema("{\"type\": \"string\"}")
                                .setData(TextNode.valueOf(value))
                                .build())
                        .setOriginalSize(0L)
                        .build()))
            .build();

    Response response =
        testEnv
            .kafkaRest()
            .target()
            .path("/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME + "/records:batch")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(request, MediaType.APPLICATION_JSON));
    assertEquals(207, response.getStatus());

    ProduceBatchResponseSuccessEntry actual = readResponseSuccessEntry(response, "1");
    ConsumerRecord<Object, Object> produced =
        testEnv
            .kafkaCluster()
            .getRecord(
                TOPIC_NAME,
                actual.getPartitionId(),
                actual.getOffset(),
                testEnv.schemaRegistry().createAvroDeserializer(),
                testEnv.schemaRegistry().createAvroDeserializer());
    assertEquals(key, produced.key());
    assertEquals(value, produced.value());
  }

  @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
  @ValueSource(strings = {"kraft", "zk"})
  public void produceAvroWithRawSchemaAndNullData_throwsBadRequest(String quorum) throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    ProduceBatchRequest request =
        ProduceBatchRequest.builder()
            .setEntries(
                Collections.singletonList(
                    ProduceBatchRequestEntry.builder()
                        .setId(new TextNode("1"))
                        .setKey(
                            ProduceRequestData.builder()
                                .setFormat(EmbeddedFormat.AVRO)
                                .setRawSchema("{\"type\": \"string\"}")
                                .setData(NullNode.getInstance())
                                .build())
                        .setValue(
                            ProduceRequestData.builder()
                                .setFormat(EmbeddedFormat.AVRO)
                                .setRawSchema("{\"type\": \"string\"}")
                                .setData(NullNode.getInstance())
                                .build())
                        .setOriginalSize(0L)
                        .build()))
            .build();

    Response response =
        testEnv
            .kafkaRest()
            .target()
            .path("/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME + "/records:batch")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(request, MediaType.APPLICATION_JSON));
    assertEquals(207, response.getStatus());

    ProduceBatchResponseSuccessEntry actual = readResponseSuccessEntry(response, "1");
    ConsumerRecord<Object, Object> produced =
        testEnv
            .kafkaCluster()
            .getRecord(
                TOPIC_NAME,
                actual.getPartitionId(),
                actual.getOffset(),
                testEnv.schemaRegistry().createAvroDeserializer(),
                testEnv.schemaRegistry().createAvroDeserializer());
    assertNull(produced.key());
    assertNull(produced.value());
  }

  @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
  @ValueSource(strings = {"kraft", "zk"})
  public void produceAvroWithRawSchemaAndInvalidData(String quorum) throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    ProduceBatchRequest request =
        ProduceBatchRequest.builder()
            .setEntries(
                Collections.singletonList(
                    ProduceBatchRequestEntry.builder()
                        .setId(new TextNode("1"))
                        .setKey(
                            ProduceRequestData.builder()
                                .setFormat(EmbeddedFormat.AVRO)
                                .setRawSchema("{\"type\": \"string\"}")
                                .setData(IntNode.valueOf(1))
                                .build())
                        .setValue(
                            ProduceRequestData.builder()
                                .setFormat(EmbeddedFormat.AVRO)
                                .setRawSchema("{\"type\": \"string\"}")
                                .setData(IntNode.valueOf(2))
                                .build())
                        .setOriginalSize(0L)
                        .build()))
            .build();

    Response response =
        testEnv
            .kafkaRest()
            .target()
            .path("/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME + "/records:batch")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(request, MediaType.APPLICATION_JSON));
    assertEquals(207, response.getStatus());

    ProduceBatchResponseFailureEntry actual = readResponseFailureEntry(response, "1");
    assertEquals(400, actual.getErrorCode());
  }

  @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
  @ValueSource(strings = {"kraft", "zk"})
  public void produceAvroWithSchemaId(String quorum) throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    SchemaKey keySchema =
        testEnv
            .schemaRegistry()
            .createSchema(DEFAULT_KEY_SUBJECT, new AvroSchema("{\"type\": \"string\"}"));
    SchemaKey valueSchema =
        testEnv
            .schemaRegistry()
            .createSchema(DEFAULT_VALUE_SUBJECT, new AvroSchema("{\"type\": \"string\"}"));
    String key = "foo";
    String value = "bar";
    ProduceBatchRequest request =
        ProduceBatchRequest.builder()
            .setEntries(
                Collections.singletonList(
                    ProduceBatchRequestEntry.builder()
                        .setId(new TextNode("1"))
                        .setKey(
                            ProduceRequestData.builder()
                                .setSchemaId(keySchema.getSchemaId())
                                .setData(TextNode.valueOf(key))
                                .build())
                        .setValue(
                            ProduceRequestData.builder()
                                .setSchemaId(valueSchema.getSchemaId())
                                .setData(TextNode.valueOf(value))
                                .build())
                        .setOriginalSize(0L)
                        .build()))
            .build();

    Response response =
        testEnv
            .kafkaRest()
            .target()
            .path("/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME + "/records:batch")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(request, MediaType.APPLICATION_JSON));
    assertEquals(207, response.getStatus());

    ProduceBatchResponseSuccessEntry actual = readResponseSuccessEntry(response, "1");
    ConsumerRecord<Object, Object> produced =
        testEnv
            .kafkaCluster()
            .getRecord(
                TOPIC_NAME,
                actual.getPartitionId(),
                actual.getOffset(),
                testEnv.schemaRegistry().createAvroDeserializer(),
                testEnv.schemaRegistry().createAvroDeserializer());
    assertEquals(key, produced.key());
    assertEquals(value, produced.value());
  }

  @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
  @ValueSource(strings = {"kraft", "zk"})
  public void produceAvroWithSchemaVersion(String quorum) throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    SchemaKey keySchema =
        testEnv
            .schemaRegistry()
            .createSchema(DEFAULT_KEY_SUBJECT, new AvroSchema("{\"type\": \"string\"}"));
    SchemaKey valueSchema =
        testEnv
            .schemaRegistry()
            .createSchema(DEFAULT_VALUE_SUBJECT, new AvroSchema("{\"type\": \"string\"}"));
    String key = "foo";
    String value = "bar";
    ProduceBatchRequest request =
        ProduceBatchRequest.builder()
            .setEntries(
                Collections.singletonList(
                    ProduceBatchRequestEntry.builder()
                        .setId(new TextNode("1"))
                        .setKey(
                            ProduceRequestData.builder()
                                .setSchemaVersion(keySchema.getSchemaVersion())
                                .setData(TextNode.valueOf(key))
                                .build())
                        .setValue(
                            ProduceRequestData.builder()
                                .setSchemaVersion(valueSchema.getSchemaVersion())
                                .setData(TextNode.valueOf(value))
                                .build())
                        .setOriginalSize(0L)
                        .build()))
            .build();

    Response response =
        testEnv
            .kafkaRest()
            .target()
            .path("/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME + "/records:batch")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(request, MediaType.APPLICATION_JSON));
    assertEquals(207, response.getStatus());

    ProduceBatchResponseSuccessEntry actual = readResponseSuccessEntry(response, "1");
    ConsumerRecord<Object, Object> produced =
        testEnv
            .kafkaCluster()
            .getRecord(
                TOPIC_NAME,
                actual.getPartitionId(),
                actual.getOffset(),
                testEnv.schemaRegistry().createAvroDeserializer(),
                testEnv.schemaRegistry().createAvroDeserializer());
    assertEquals(key, produced.key());
    assertEquals(value, produced.value());
  }

  @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
  @ValueSource(strings = {"kraft", "zk"})
  public void produceAvroWithLatestSchema(String quorum) throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    testEnv
        .schemaRegistry()
        .createSchema(DEFAULT_KEY_SUBJECT, new AvroSchema("{\"type\": \"string\"}"));
    testEnv
        .schemaRegistry()
        .createSchema(DEFAULT_VALUE_SUBJECT, new AvroSchema("{\"type\": \"string\"}"));
    String key = "foo";
    String value = "bar";
    ProduceBatchRequest request =
        ProduceBatchRequest.builder()
            .setEntries(
                Collections.singletonList(
                    ProduceBatchRequestEntry.builder()
                        .setId(new TextNode("1"))
                        .setKey(ProduceRequestData.builder().setData(TextNode.valueOf(key)).build())
                        .setValue(
                            ProduceRequestData.builder().setData(TextNode.valueOf(value)).build())
                        .setOriginalSize(0L)
                        .build()))
            .build();

    Response response =
        testEnv
            .kafkaRest()
            .target()
            .path("/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME + "/records:batch")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(request, MediaType.APPLICATION_JSON));
    assertEquals(207, response.getStatus());

    ProduceBatchResponseSuccessEntry actual = readResponseSuccessEntry(response, "1");
    ConsumerRecord<Object, Object> produced =
        testEnv
            .kafkaCluster()
            .getRecord(
                TOPIC_NAME,
                actual.getPartitionId(),
                actual.getOffset(),
                testEnv.schemaRegistry().createAvroDeserializer(),
                testEnv.schemaRegistry().createAvroDeserializer());
    assertEquals(key, produced.key());
    assertEquals(value, produced.value());
  }

  @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
  @ValueSource(strings = {"kraft", "zk"})
  public void produceAvroWithRawSchemaAndSubject(String quorum) throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    String key = "foo";
    String value = "bar";
    ProduceBatchRequest request =
        ProduceBatchRequest.builder()
            .setEntries(
                Collections.singletonList(
                    ProduceBatchRequestEntry.builder()
                        .setId(new TextNode("1"))
                        .setKey(
                            ProduceRequestData.builder()
                                .setFormat(EmbeddedFormat.AVRO)
                                .setSubject("my-key-subject")
                                .setRawSchema("{\"type\": \"string\"}")
                                .setData(TextNode.valueOf(key))
                                .build())
                        .setValue(
                            ProduceRequestData.builder()
                                .setFormat(EmbeddedFormat.AVRO)
                                .setSubject("my-value-subject")
                                .setRawSchema("{\"type\": \"string\"}")
                                .setData(TextNode.valueOf(value))
                                .build())
                        .setOriginalSize(0L)
                        .build()))
            .build();

    Response response =
        testEnv
            .kafkaRest()
            .target()
            .path("/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME + "/records:batch")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(request, MediaType.APPLICATION_JSON));
    assertEquals(207, response.getStatus());

    ProduceBatchResponseSuccessEntry actual = readResponseSuccessEntry(response, "1");
    ConsumerRecord<Object, Object> produced =
        testEnv
            .kafkaCluster()
            .getRecord(
                TOPIC_NAME,
                actual.getPartitionId(),
                actual.getOffset(),
                testEnv.schemaRegistry().createAvroDeserializer(),
                testEnv.schemaRegistry().createAvroDeserializer());
    assertEquals(key, produced.key());
    assertEquals(value, produced.value());
  }

  @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
  @ValueSource(strings = {"kraft", "zk"})
  public void produceAvroWithSchemaIdAndSubject(String quorum) throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    SchemaKey keySchema =
        testEnv
            .schemaRegistry()
            .createSchema("my-key-subject", new AvroSchema("{\"type\": \"string\"}"));
    SchemaKey valueSchema =
        testEnv
            .schemaRegistry()
            .createSchema("my-value-subject", new AvroSchema("{\"type\": \"string\"}"));
    String key = "foo";
    String value = "bar";
    ProduceBatchRequest request =
        ProduceBatchRequest.builder()
            .setEntries(
                Collections.singletonList(
                    ProduceBatchRequestEntry.builder()
                        .setId(new TextNode("1"))
                        .setKey(
                            ProduceRequestData.builder()
                                .setSubject("my-key-subject")
                                .setSchemaId(keySchema.getSchemaId())
                                .setData(TextNode.valueOf(key))
                                .build())
                        .setValue(
                            ProduceRequestData.builder()
                                .setSubject("my-value-subject")
                                .setSchemaId(valueSchema.getSchemaId())
                                .setData(TextNode.valueOf(value))
                                .build())
                        .setOriginalSize(0L)
                        .build()))
            .build();

    Response response =
        testEnv
            .kafkaRest()
            .target()
            .path("/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME + "/records:batch")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(request, MediaType.APPLICATION_JSON));
    assertEquals(207, response.getStatus());

    ProduceBatchResponseSuccessEntry actual = readResponseSuccessEntry(response, "1");
    ConsumerRecord<Object, Object> produced =
        testEnv
            .kafkaCluster()
            .getRecord(
                TOPIC_NAME,
                actual.getPartitionId(),
                actual.getOffset(),
                testEnv.schemaRegistry().createAvroDeserializer(),
                testEnv.schemaRegistry().createAvroDeserializer());
    assertEquals(key, produced.key());
    assertEquals(value, produced.value());
  }

  @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
  @ValueSource(strings = {"kraft", "zk"})
  public void produceAvroWithSchemaVersionAndSubject(String quorum) throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    SchemaKey keySchema =
        testEnv
            .schemaRegistry()
            .createSchema("my-key-subject", new AvroSchema("{\"type\": \"string\"}"));
    SchemaKey valueSchema =
        testEnv
            .schemaRegistry()
            .createSchema("my-value-subject", new AvroSchema("{\"type\": \"string\"}"));
    String key = "foo";
    String value = "bar";
    ProduceBatchRequest request =
        ProduceBatchRequest.builder()
            .setEntries(
                Collections.singletonList(
                    ProduceBatchRequestEntry.builder()
                        .setId(new TextNode("1"))
                        .setKey(
                            ProduceRequestData.builder()
                                .setSubject("my-key-subject")
                                .setSchemaVersion(keySchema.getSchemaVersion())
                                .setData(TextNode.valueOf(key))
                                .build())
                        .setValue(
                            ProduceRequestData.builder()
                                .setSubject("my-value-subject")
                                .setSchemaVersion(valueSchema.getSchemaVersion())
                                .setData(TextNode.valueOf(value))
                                .build())
                        .setOriginalSize(0L)
                        .build()))
            .build();

    Response response =
        testEnv
            .kafkaRest()
            .target()
            .path("/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME + "/records:batch")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(request, MediaType.APPLICATION_JSON));
    assertEquals(207, response.getStatus());

    ProduceBatchResponseSuccessEntry actual = readResponseSuccessEntry(response, "1");
    ConsumerRecord<Object, Object> produced =
        testEnv
            .kafkaCluster()
            .getRecord(
                TOPIC_NAME,
                actual.getPartitionId(),
                actual.getOffset(),
                testEnv.schemaRegistry().createAvroDeserializer(),
                testEnv.schemaRegistry().createAvroDeserializer());
    assertEquals(key, produced.key());
    assertEquals(value, produced.value());
  }

  @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
  @ValueSource(strings = {"kraft", "zk"})
  public void produceAvroWithLatestSchemaAndSubject(String quorum) throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    testEnv
        .schemaRegistry()
        .createSchema("my-key-subject", new AvroSchema("{\"type\": \"string\"}"));
    testEnv
        .schemaRegistry()
        .createSchema("my-value-subject", new AvroSchema("{\"type\": \"string\"}"));
    String key = "foo";
    String value = "bar";
    ProduceBatchRequest request =
        ProduceBatchRequest.builder()
            .setEntries(
                Collections.singletonList(
                    ProduceBatchRequestEntry.builder()
                        .setId(new TextNode("1"))
                        .setKey(
                            ProduceRequestData.builder()
                                .setSubject("my-key-subject")
                                .setData(TextNode.valueOf(key))
                                .build())
                        .setValue(
                            ProduceRequestData.builder()
                                .setSubject("my-value-subject")
                                .setData(TextNode.valueOf(value))
                                .build())
                        .setOriginalSize(0L)
                        .build()))
            .build();

    Response response =
        testEnv
            .kafkaRest()
            .target()
            .path("/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME + "/records:batch")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(request, MediaType.APPLICATION_JSON));
    assertEquals(207, response.getStatus());

    ProduceBatchResponseSuccessEntry actual = readResponseSuccessEntry(response, "1");
    ConsumerRecord<Object, Object> produced =
        testEnv
            .kafkaCluster()
            .getRecord(
                TOPIC_NAME,
                actual.getPartitionId(),
                actual.getOffset(),
                testEnv.schemaRegistry().createAvroDeserializer(),
                testEnv.schemaRegistry().createAvroDeserializer());
    assertEquals(key, produced.key());
    assertEquals(value, produced.value());
  }

  @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
  @ValueSource(strings = {"kraft", "zk"})
  public void produceAvroWithRawSchemaAndSubjectStrategy(String quorum) throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    String keyRawSchema =
        "{\"type\": \"record\", \"name\": \"MyKey\", \"fields\": [{\"name\": \"foo\", \"type\": "
            + "\"string\"}]}";
    String valueRawSchema =
        "{\"type\": \"record\", \"name\": \"MyValue\", \"fields\": [{\"name\": \"bar\", \"type\": "
            + "\"string\"}]}";
    ObjectNode key = new ObjectNode(JsonNodeFactory.instance);
    key.put("foo", "foz");
    ObjectNode value = new ObjectNode(JsonNodeFactory.instance);
    value.put("bar", "baz");
    ProduceBatchRequest request =
        ProduceBatchRequest.builder()
            .setEntries(
                Collections.singletonList(
                    ProduceBatchRequestEntry.builder()
                        .setId(new TextNode("1"))
                        .setKey(
                            ProduceRequestData.builder()
                                .setFormat(EmbeddedFormat.AVRO)
                                .setSubjectNameStrategy(EnumSubjectNameStrategy.RECORD_NAME)
                                .setRawSchema(keyRawSchema)
                                .setData(key)
                                .build())
                        .setValue(
                            ProduceRequestData.builder()
                                .setFormat(EmbeddedFormat.AVRO)
                                .setSubjectNameStrategy(EnumSubjectNameStrategy.RECORD_NAME)
                                .setRawSchema(valueRawSchema)
                                .setData(value)
                                .build())
                        .setOriginalSize(0L)
                        .build()))
            .build();

    Response response =
        testEnv
            .kafkaRest()
            .target()
            .path("/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME + "/records:batch")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(request, MediaType.APPLICATION_JSON));
    assertEquals(207, response.getStatus());

    ProduceBatchResponseSuccessEntry actual = readResponseSuccessEntry(response, "1");
    ConsumerRecord<Object, Object> produced =
        testEnv
            .kafkaCluster()
            .getRecord(
                TOPIC_NAME,
                actual.getPartitionId(),
                actual.getOffset(),
                testEnv.schemaRegistry().createAvroDeserializer(),
                testEnv.schemaRegistry().createAvroDeserializer());
    GenericRecord expectedKey = new GenericData.Record(new AvroSchema(keyRawSchema).rawSchema());
    expectedKey.put("foo", "foz");
    GenericRecord expectedValue =
        new GenericData.Record(new AvroSchema(valueRawSchema).rawSchema());
    expectedValue.put("bar", "baz");
    assertEquals(expectedKey, produced.key());
    assertEquals(expectedValue, produced.value());
  }

  @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
  @ValueSource(strings = {"kraft", "zk"})
  public void produceAvroWithSchemaIdAndSubjectStrategy(String quorum) throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    AvroSchema keySchema =
        new AvroSchema(
            "{\"type\": \"record\", \"name\": \"MyKey\", \"fields\": [{\"name\": \"foo\", "
                + "\"type\": \"string\"}]}");
    String keySubject =
        new RecordNameStrategy().subjectName(TOPIC_NAME, /* isKey= */ true, keySchema);
    SchemaKey keySchemaKey = testEnv.schemaRegistry().createSchema(keySubject, keySchema);
    AvroSchema valueSchema =
        new AvroSchema(
            "{\"type\": \"record\", \"name\": \"MyValue\", \"fields\": [{\"name\": \"bar\", "
                + "\"type\": \"string\"}]}");
    String valueSubject =
        new RecordNameStrategy().subjectName(TOPIC_NAME, /* isKey= */ false, valueSchema);
    SchemaKey valueSchemaKey = testEnv.schemaRegistry().createSchema(valueSubject, valueSchema);
    ObjectNode key = new ObjectNode(JsonNodeFactory.instance);
    key.put("foo", "foz");
    ObjectNode value = new ObjectNode(JsonNodeFactory.instance);
    value.put("bar", "baz");
    ProduceBatchRequest request =
        ProduceBatchRequest.builder()
            .setEntries(
                Collections.singletonList(
                    ProduceBatchRequestEntry.builder()
                        .setId(new TextNode("1"))
                        .setKey(
                            ProduceRequestData.builder()
                                .setSubjectNameStrategy(EnumSubjectNameStrategy.RECORD_NAME)
                                .setSchemaId(keySchemaKey.getSchemaId())
                                .setData(key)
                                .build())
                        .setValue(
                            ProduceRequestData.builder()
                                .setSubjectNameStrategy(EnumSubjectNameStrategy.RECORD_NAME)
                                .setSchemaId(valueSchemaKey.getSchemaId())
                                .setData(value)
                                .build())
                        .setOriginalSize(0L)
                        .build()))
            .build();

    Response response =
        testEnv
            .kafkaRest()
            .target()
            .path("/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME + "/records:batch")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(request, MediaType.APPLICATION_JSON));
    assertEquals(207, response.getStatus());

    ProduceBatchResponseSuccessEntry actual = readResponseSuccessEntry(response, "1");
    ConsumerRecord<Object, Object> produced =
        testEnv
            .kafkaCluster()
            .getRecord(
                TOPIC_NAME,
                actual.getPartitionId(),
                actual.getOffset(),
                testEnv.schemaRegistry().createAvroDeserializer(),
                testEnv.schemaRegistry().createAvroDeserializer());
    GenericRecord expectedKey = new GenericData.Record(keySchema.rawSchema());
    expectedKey.put("foo", "foz");
    GenericRecord expectedValue = new GenericData.Record(valueSchema.rawSchema());
    expectedValue.put("bar", "baz");
    assertEquals(expectedKey, produced.key());
    assertEquals(expectedValue, produced.value());
  }

  @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
  @ValueSource(strings = {"kraft", "zk"})
  public void produceAvroWithSchemaVersionAndSubjectStrategy(String quorum) throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    AvroSchema keySchema =
        new AvroSchema(
            "{\"type\": \"record\", \"name\": \"MyKey\", \"fields\": [{\"name\": \"foo\", "
                + "\"type\": \"string\"}]}");
    String keySubject =
        new TopicNameStrategy().subjectName(TOPIC_NAME, /* isKey= */ true, keySchema);
    SchemaKey keySchemaKey = testEnv.schemaRegistry().createSchema(keySubject, keySchema);
    AvroSchema valueSchema =
        new AvroSchema(
            "{\"type\": \"record\", \"name\": \"MyValue\", \"fields\": [{\"name\": \"bar\", "
                + "\"type\": \"string\"}]}");
    String valueSubject =
        new TopicNameStrategy().subjectName(TOPIC_NAME, /* isKey= */ false, valueSchema);
    SchemaKey valueSchemaKey = testEnv.schemaRegistry().createSchema(valueSubject, valueSchema);
    ObjectNode key = new ObjectNode(JsonNodeFactory.instance);
    key.put("foo", "foz");
    ObjectNode value = new ObjectNode(JsonNodeFactory.instance);
    value.put("bar", "baz");
    ProduceBatchRequest request =
        ProduceBatchRequest.builder()
            .setEntries(
                Collections.singletonList(
                    ProduceBatchRequestEntry.builder()
                        .setId(new TextNode("1"))
                        .setKey(
                            ProduceRequestData.builder()
                                .setSubjectNameStrategy(EnumSubjectNameStrategy.TOPIC_NAME)
                                .setSchemaVersion(keySchemaKey.getSchemaVersion())
                                .setData(key)
                                .build())
                        .setValue(
                            ProduceRequestData.builder()
                                .setSubjectNameStrategy(EnumSubjectNameStrategy.TOPIC_NAME)
                                .setSchemaVersion(valueSchemaKey.getSchemaVersion())
                                .setData(value)
                                .build())
                        .setOriginalSize(0L)
                        .build()))
            .build();

    Response response =
        testEnv
            .kafkaRest()
            .target()
            .path("/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME + "/records:batch")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(request, MediaType.APPLICATION_JSON));
    assertEquals(207, response.getStatus());

    ProduceBatchResponseSuccessEntry actual = readResponseSuccessEntry(response, "1");
    ConsumerRecord<Object, Object> produced =
        testEnv
            .kafkaCluster()
            .getRecord(
                TOPIC_NAME,
                actual.getPartitionId(),
                actual.getOffset(),
                testEnv.schemaRegistry().createAvroDeserializer(),
                testEnv.schemaRegistry().createAvroDeserializer());
    GenericRecord expectedKey = new GenericData.Record(keySchema.rawSchema());
    expectedKey.put("foo", "foz");
    GenericRecord expectedValue = new GenericData.Record(valueSchema.rawSchema());
    expectedValue.put("bar", "baz");
    assertEquals(expectedKey, produced.key());
    assertEquals(expectedValue, produced.value());
  }

  @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
  @ValueSource(strings = {"kraft", "zk"})
  public void produceAvroWithLatestSchemaAndSubjectStrategy(String quorum) throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    AvroSchema keySchema =
        new AvroSchema(
            "{\"type\": \"record\", \"name\": \"MyKey\", \"fields\": [{\"name\": \"foo\", "
                + "\"type\": \"string\"}]}");
    String keySubject =
        new TopicNameStrategy().subjectName(TOPIC_NAME, /* isKey= */ true, keySchema);
    testEnv.schemaRegistry().createSchema(keySubject, keySchema);
    AvroSchema valueSchema =
        new AvroSchema(
            "{\"type\": \"record\", \"name\": \"MyValue\", \"fields\": [{\"name\": \"bar\", "
                + "\"type\": \"string\"}]}");
    String valueSubject =
        new TopicNameStrategy().subjectName(TOPIC_NAME, /* isKey= */ false, valueSchema);
    testEnv.schemaRegistry().createSchema(valueSubject, valueSchema);
    ObjectNode key = new ObjectNode(JsonNodeFactory.instance);
    key.put("foo", "foz");
    ObjectNode value = new ObjectNode(JsonNodeFactory.instance);
    value.put("bar", "baz");
    ProduceBatchRequest request =
        ProduceBatchRequest.builder()
            .setEntries(
                Collections.singletonList(
                    ProduceBatchRequestEntry.builder()
                        .setId(new TextNode("1"))
                        .setKey(
                            ProduceRequestData.builder()
                                .setSubjectNameStrategy(EnumSubjectNameStrategy.TOPIC_NAME)
                                .setData(key)
                                .build())
                        .setValue(
                            ProduceRequestData.builder()
                                .setSubjectNameStrategy(EnumSubjectNameStrategy.TOPIC_NAME)
                                .setData(value)
                                .build())
                        .setOriginalSize(0L)
                        .build()))
            .build();

    Response response =
        testEnv
            .kafkaRest()
            .target()
            .path("/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME + "/records:batch")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(request, MediaType.APPLICATION_JSON));
    assertEquals(207, response.getStatus());

    ProduceBatchResponseSuccessEntry actual = readResponseSuccessEntry(response, "1");
    ConsumerRecord<Object, Object> produced =
        testEnv
            .kafkaCluster()
            .getRecord(
                TOPIC_NAME,
                actual.getPartitionId(),
                actual.getOffset(),
                testEnv.schemaRegistry().createAvroDeserializer(),
                testEnv.schemaRegistry().createAvroDeserializer());
    GenericRecord expectedKey = new GenericData.Record(keySchema.rawSchema());
    expectedKey.put("foo", "foz");
    GenericRecord expectedValue = new GenericData.Record(valueSchema.rawSchema());
    expectedValue.put("bar", "baz");
    assertEquals(expectedKey, produced.key());
    assertEquals(expectedValue, produced.value());
  }

  @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
  @ValueSource(strings = {"kraft", "zk"})
  public void produceStringWithPartitionId(String quorum) throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    int partitionId = 1;
    String key = "foo";
    String value = "bar";
    ProduceBatchRequest request =
        ProduceBatchRequest.builder()
            .setEntries(
                Collections.singletonList(
                    ProduceBatchRequestEntry.builder()
                        .setId(new TextNode("1"))
                        .setPartitionId(partitionId)
                        .setKey(
                            ProduceRequestData.builder()
                                .setFormat(EmbeddedFormat.STRING)
                                .setData(TextNode.valueOf(key))
                                .build())
                        .setValue(
                            ProduceRequestData.builder()
                                .setFormat(EmbeddedFormat.STRING)
                                .setData(TextNode.valueOf(value))
                                .build())
                        .setOriginalSize(0L)
                        .build()))
            .build();

    Response response =
        testEnv
            .kafkaRest()
            .target()
            .path("/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME + "/records:batch")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(request, MediaType.APPLICATION_JSON));
    assertEquals(207, response.getStatus());

    ProduceBatchResponseSuccessEntry actual = readResponseSuccessEntry(response, "1");
    ConsumerRecord<String, String> produced =
        testEnv
            .kafkaCluster()
            .getRecord(
                TOPIC_NAME,
                partitionId,
                actual.getOffset(),
                new StringDeserializer(),
                new StringDeserializer());
    assertEquals(key, produced.key());
    assertEquals(value, produced.value());
  }

  @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
  @ValueSource(strings = {"kraft", "zk"})
  public void produceStringWithTimestamp(String quorum) throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    Instant timestamp = Instant.ofEpochMilli(1000);
    String key = "foo";
    String value = "bar";
    ProduceBatchRequest request =
        ProduceBatchRequest.builder()
            .setEntries(
                Collections.singletonList(
                    ProduceBatchRequestEntry.builder()
                        .setId(new TextNode("1"))
                        .setKey(
                            ProduceRequestData.builder()
                                .setFormat(EmbeddedFormat.STRING)
                                .setData(TextNode.valueOf(key))
                                .build())
                        .setValue(
                            ProduceRequestData.builder()
                                .setFormat(EmbeddedFormat.STRING)
                                .setData(TextNode.valueOf(value))
                                .build())
                        .setTimestamp(timestamp)
                        .setOriginalSize(0L)
                        .build()))
            .build();

    Response response =
        testEnv
            .kafkaRest()
            .target()
            .path("/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME + "/records:batch")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(request, MediaType.APPLICATION_JSON));
    assertEquals(207, response.getStatus());

    ProduceBatchResponseSuccessEntry actual = readResponseSuccessEntry(response, "1");
    ConsumerRecord<String, String> produced =
        testEnv
            .kafkaCluster()
            .getRecord(
                TOPIC_NAME,
                actual.getPartitionId(),
                actual.getOffset(),
                new StringDeserializer(),
                new StringDeserializer());
    assertEquals(key, produced.key());
    assertEquals(value, produced.value());
    assertEquals(timestamp, Instant.ofEpochMilli(produced.timestamp()));
  }

  @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
  @ValueSource(strings = {"kraft", "zk"})
  public void produceStringWithHeaders(String quorum) throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    String key = "foo";
    String value = "bar";
    ProduceBatchRequest request =
        ProduceBatchRequest.builder()
            .setEntries(
                Collections.singletonList(
                    ProduceBatchRequestEntry.builder()
                        .setId(new TextNode("1"))
                        .setHeaders(
                            Arrays.asList(
                                ProduceRequestHeader.create(
                                    "header-1", ByteString.copyFromUtf8("value-1")),
                                ProduceRequestHeader.create(
                                    "header-1", ByteString.copyFromUtf8("value-2")),
                                ProduceRequestHeader.create(
                                    "header-2", ByteString.copyFromUtf8("value-3"))))
                        .setKey(
                            ProduceRequestData.builder()
                                .setFormat(EmbeddedFormat.STRING)
                                .setData(TextNode.valueOf(key))
                                .build())
                        .setValue(
                            ProduceRequestData.builder()
                                .setFormat(EmbeddedFormat.STRING)
                                .setData(TextNode.valueOf(value))
                                .build())
                        .setOriginalSize(0L)
                        .build()))
            .build();

    Response response =
        testEnv
            .kafkaRest()
            .target()
            .path("/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME + "/records:batch")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(request, MediaType.APPLICATION_JSON));
    assertEquals(207, response.getStatus());

    ProduceBatchResponseSuccessEntry actual = readResponseSuccessEntry(response, "1");
    ConsumerRecord<String, String> produced =
        testEnv
            .kafkaCluster()
            .getRecord(
                TOPIC_NAME,
                actual.getPartitionId(),
                actual.getOffset(),
                new StringDeserializer(),
                new StringDeserializer());
    assertEquals(key, produced.key());
    assertEquals(value, produced.value());
    assertEquals(
        Arrays.asList(
            new RecordHeader("header-1", ByteString.copyFromUtf8("value-1").toByteArray()),
            new RecordHeader("header-1", ByteString.copyFromUtf8("value-2").toByteArray())),
        ImmutableList.copyOf(produced.headers().headers("header-1")));
    assertEquals(
        Collections.singletonList(
            new RecordHeader("header-2", ByteString.copyFromUtf8("value-3").toByteArray())),
        ImmutableList.copyOf(produced.headers().headers("header-2")));
  }

  @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
  @ValueSource(strings = {"kraft", "zk"})
  public void produceStringAndAvro(String quorum) throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    String key = "foo";
    String value = "bar";
    ProduceBatchRequest request =
        ProduceBatchRequest.builder()
            .setEntries(
                Collections.singletonList(
                    ProduceBatchRequestEntry.builder()
                        .setId(new TextNode("1"))
                        .setKey(
                            ProduceRequestData.builder()
                                .setFormat(EmbeddedFormat.STRING)
                                .setData(TextNode.valueOf(key))
                                .build())
                        .setValue(
                            ProduceRequestData.builder()
                                .setFormat(EmbeddedFormat.AVRO)
                                .setRawSchema("{\"type\": \"string\"}")
                                .setData(TextNode.valueOf(value))
                                .build())
                        .setOriginalSize(0L)
                        .build()))
            .build();

    Response response =
        testEnv
            .kafkaRest()
            .target()
            .path("/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME + "/records:batch")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(request, MediaType.APPLICATION_JSON));
    assertEquals(207, response.getStatus());

    ProduceBatchResponseSuccessEntry actual = readResponseSuccessEntry(response, "1");
    ConsumerRecord<String, Object> produced =
        testEnv
            .kafkaCluster()
            .getRecord(
                TOPIC_NAME,
                actual.getPartitionId(),
                actual.getOffset(),
                new StringDeserializer(),
                testEnv.schemaRegistry().createAvroDeserializer());
    assertEquals(key, produced.key());
    assertEquals(value, produced.value());
  }

  @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
  @ValueSource(strings = {"kraft", "zk"})
  public void produceStringKeyOnly(String quorum) throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    String key = "foo";
    ProduceBatchRequest request =
        ProduceBatchRequest.builder()
            .setEntries(
                Collections.singletonList(
                    ProduceBatchRequestEntry.builder()
                        .setId(new TextNode("1"))
                        .setKey(
                            ProduceRequestData.builder()
                                .setFormat(EmbeddedFormat.STRING)
                                .setData(TextNode.valueOf(key))
                                .build())
                        .setOriginalSize(0L)
                        .build()))
            .build();

    Response response =
        testEnv
            .kafkaRest()
            .target()
            .path("/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME + "/records:batch")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(request, MediaType.APPLICATION_JSON));
    assertEquals(207, response.getStatus());

    ProduceBatchResponseSuccessEntry actual = readResponseSuccessEntry(response, "1");
    ConsumerRecord<String, byte[]> produced =
        testEnv
            .kafkaCluster()
            .getRecord(
                TOPIC_NAME,
                actual.getPartitionId(),
                actual.getOffset(),
                new StringDeserializer(),
                new ByteArrayDeserializer());
    assertEquals(key, produced.key());
    assertNull(produced.value());
  }

  @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
  @ValueSource(strings = {"kraft", "zk"})
  public void produceStringValueOnly(String quorum) throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    String value = "bar";
    ProduceBatchRequest request =
        ProduceBatchRequest.builder()
            .setEntries(
                Collections.singletonList(
                    ProduceBatchRequestEntry.builder()
                        .setId(new TextNode("1"))
                        .setValue(
                            ProduceRequestData.builder()
                                .setFormat(EmbeddedFormat.STRING)
                                .setData(TextNode.valueOf(value))
                                .build())
                        .setOriginalSize(0L)
                        .build()))
            .build();

    Response response =
        testEnv
            .kafkaRest()
            .target()
            .path("/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME + "/records:batch")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(request, MediaType.APPLICATION_JSON));
    assertEquals(207, response.getStatus());

    ProduceBatchResponseSuccessEntry actual = readResponseSuccessEntry(response, "1");
    ConsumerRecord<String, String> produced =
        testEnv
            .kafkaCluster()
            .getRecord(
                TOPIC_NAME,
                actual.getPartitionId(),
                actual.getOffset(),
                new StringDeserializer(),
                new StringDeserializer());
    assertNull(produced.key());
    assertEquals(value, produced.value());
  }

  @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
  @ValueSource(strings = {"kraft", "zk"})
  public void produceNothing(String quorum) throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    ProduceBatchRequest request =
        ProduceBatchRequest.builder()
            .setEntries(
                Collections.singletonList(
                    ProduceBatchRequestEntry.builder()
                        .setId(new TextNode("1"))
                        .setOriginalSize(0L)
                        .build()))
            .build();

    Response response =
        testEnv
            .kafkaRest()
            .target()
            .path("/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME + "/records:batch")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(request, MediaType.APPLICATION_JSON));
    assertEquals(207, response.getStatus());

    ProduceBatchResponseSuccessEntry actual = readResponseSuccessEntry(response, "1");
    ConsumerRecord<byte[], byte[]> produced =
        testEnv
            .kafkaCluster()
            .getRecord(
                TOPIC_NAME,
                actual.getPartitionId(),
                actual.getOffset(),
                new ByteArrayDeserializer(),
                new ByteArrayDeserializer());
    assertNull(produced.key());
    assertNull(produced.value());
  }

  @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
  @ValueSource(strings = {"kraft", "zk"})
  public void produceJsonBatch(String quorum) throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    ProduceBatchRequestEntry[] entries = new ProduceBatchRequestEntry[10];
    for (int i = 0; i < 10; i++) {
      entries[i] =
          ProduceBatchRequestEntry.builder()
              .setId(new TextNode(Integer.toString(i)))
              .setPartitionId(0)
              .setKey(
                  ProduceRequestData.builder()
                      .setFormat(EmbeddedFormat.JSON)
                      .setData(TextNode.valueOf("key-" + i))
                      .build())
              .setValue(
                  ProduceRequestData.builder()
                      .setFormat(EmbeddedFormat.JSON)
                      .setData(TextNode.valueOf("value-" + i))
                      .build())
              .setOriginalSize(0L)
              .build();
    }

    ProduceBatchRequest request =
        ProduceBatchRequest.builder().setEntries(Arrays.asList(entries)).build();

    Response response =
        testEnv
            .kafkaRest()
            .target()
            .path("/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME + "/records:batch")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(request, MediaType.APPLICATION_JSON));
    assertEquals(207, response.getStatus());

    List<ProduceBatchResponseSuccessEntry> actual = readResponseSuccesses(response);
    KafkaJsonDeserializer<Object> deserializer = new KafkaJsonDeserializer<>();
    deserializer.configure(Collections.emptyMap(), /* isKey= */ false);

    ConsumerRecords<Object, Object> producedRecords =
        testEnv.kafkaCluster().getRecords(TOPIC_NAME, deserializer, deserializer, 10);

    assertEquals(10, producedRecords.count());
    ArrayList<ConsumerRecord<Object, Object>> recordsList = new ArrayList<>();
    producedRecords.forEach(recordsList::add);

    actual.forEach(
        result -> {
          int id = Integer.parseInt(result.getId());
          ConsumerRecord<Object, Object> record = recordsList.get(id);

          assertEquals(id, record.offset());
          assertEquals(result.getPartitionId(), record.partition());
          assertEquals(result.getOffset(), record.offset());

          assertEquals(
              entries[id]
                  .getKey()
                  .map(ProduceRequestData::getData)
                  .map(JsonNode::asText)
                  .orElse(null),
              record.key());
          assertEquals(
              entries[id]
                  .getValue()
                  .map(ProduceRequestData::getData)
                  .map(JsonNode::asText)
                  .orElse(null),
              record.value());
        });
  }

  @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
  @ValueSource(strings = {"kraft", "zk"})
  public void produceStringBatch(String quorum) throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    ProduceBatchRequestEntry[] entries = new ProduceBatchRequestEntry[10];
    for (int i = 0; i < 10; i++) {
      entries[i] =
          ProduceBatchRequestEntry.builder()
              .setId(new TextNode(Integer.toString(i)))
              .setPartitionId(0)
              .setKey(
                  ProduceRequestData.builder()
                      .setFormat(EmbeddedFormat.STRING)
                      .setData(TextNode.valueOf("key-" + i))
                      .build())
              .setValue(
                  ProduceRequestData.builder()
                      .setFormat(EmbeddedFormat.STRING)
                      .setData(TextNode.valueOf("value-" + i))
                      .build())
              .setOriginalSize(0L)
              .build();
    }

    ProduceBatchRequest request =
        ProduceBatchRequest.builder().setEntries(Arrays.asList(entries)).build();

    Response response =
        testEnv
            .kafkaRest()
            .target()
            .path("/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME + "/records:batch")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(request, MediaType.APPLICATION_JSON));
    assertEquals(207, response.getStatus());

    List<ProduceBatchResponseSuccessEntry> actual = readResponseSuccesses(response);
    StringDeserializer deserializer = new StringDeserializer();
    deserializer.configure(Collections.emptyMap(), /* isKey= */ false);

    ConsumerRecords<String, String> producedRecords =
        testEnv.kafkaCluster().getRecords(TOPIC_NAME, deserializer, deserializer, 10);

    assertEquals(10, producedRecords.count());
    ArrayList<ConsumerRecord<String, String>> recordsList = new ArrayList<>();
    producedRecords.forEach(recordsList::add);

    actual.forEach(
        result -> {
          int id = Integer.parseInt(result.getId());
          ConsumerRecord<String, String> record = recordsList.get(id);

          assertEquals(id, record.offset());
          assertEquals(result.getPartitionId(), record.partition());
          assertEquals(result.getOffset(), record.offset());

          assertEquals(
              entries[id]
                  .getKey()
                  .map(ProduceRequestData::getData)
                  .map(JsonNode::asText)
                  .orElse(null),
              record.key());
          assertEquals(
              entries[id]
                  .getValue()
                  .map(ProduceRequestData::getData)
                  .map(JsonNode::asText)
                  .orElse(null),
              record.value());
        });
  }

  @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
  @ValueSource(strings = {"kraft", "zk"})
  public void produceBinaryBatchWithInvalidData_throwsMultipleBadRequests(String quorum)
      throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    ProduceBatchRequestEntry[] entries = new ProduceBatchRequestEntry[10];
    for (int i = 0; i < 10; i++) {
      entries[i] =
          ProduceBatchRequestEntry.builder()
              .setId(new TextNode(Integer.toString(i)))
              .setPartitionId(0)
              .setKey(
                  ProduceRequestData.builder()
                      .setFormat(EmbeddedFormat.BINARY)
                      .setData(IntNode.valueOf(2 * i))
                      .build())
              .setValue(
                  ProduceRequestData.builder()
                      .setFormat(EmbeddedFormat.BINARY)
                      .setData(IntNode.valueOf(2 * i + 1))
                      .build())
              .setOriginalSize(0L)
              .build();
    }

    ProduceBatchRequest request =
        ProduceBatchRequest.builder().setEntries(Arrays.asList(entries)).build();

    Response response =
        testEnv
            .kafkaRest()
            .target()
            .path("/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME + "/records:batch")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(request, MediaType.APPLICATION_JSON));
    assertEquals(207, response.getStatus());

    List<ProduceBatchResponseFailureEntry> actual = readResponseFailures(response);
    assertEquals(10, actual.size());
    for (int i = 0; i < 10; i++) {
      assertEquals(400, actual.get(i).getErrorCode());
    }
  }

  @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
  @ValueSource(strings = {"kraft", "zk"})
  public void produceBinaryWithLargerSizeMessage(String quorum) throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    ByteString key = ByteString.copyFromUtf8("foo");
    // Kafka server and producer is configured to accept messages upto 20971520 Bytes (20MB) but
    // KafkaProducer calculates produced bytes including key, value, headers size and additional
    // record overhead bytes hence producing message of 20971420 bytes.
    int valueSize = ((2 << 20) * 10) - 100;
    byte[] value = generateBinaryData(valueSize);
    ProduceBatchRequest request =
        ProduceBatchRequest.builder()
            .setEntries(
                Collections.singletonList(
                    ProduceBatchRequestEntry.builder()
                        .setId(new TextNode("1"))
                        .setKey(
                            ProduceRequestData.builder()
                                .setFormat(EmbeddedFormat.BINARY)
                                .setData(BinaryNode.valueOf(key.toByteArray()))
                                .build())
                        .setValue(
                            ProduceRequestData.builder()
                                .setFormat(EmbeddedFormat.BINARY)
                                .setData(BinaryNode.valueOf(value))
                                .build())
                        .setOriginalSize(0L)
                        .build()))
            .build();

    Response response =
        testEnv
            .kafkaRest()
            .target()
            .path("/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME + "/records:batch")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(request, MediaType.APPLICATION_JSON));
    assertEquals(207, response.getStatus());

    ProduceBatchResponseSuccessEntry actual = readResponseSuccessEntry(response, "1");
    assertTrue(actual.getValue().isPresent());
    assertEquals(valueSize, actual.getValue().get().getSize());

    ConsumerRecord<byte[], byte[]> produced =
        testEnv
            .kafkaCluster()
            .getRecord(
                TOPIC_NAME,
                actual.getPartitionId(),
                actual.getOffset(),
                new ByteArrayDeserializer(),
                new ByteArrayDeserializer());
    assertEquals(key, ByteString.copyFrom(produced.key()));
    assertEquals(valueSize, produced.serializedValueSize());
    assertEquals(Arrays.toString(value), Arrays.toString(produced.value()));
  }

  private void doByteLimitReachedTest() throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    String key = "foo";
    String value = "bar";
    ProduceBatchRequest request =
        ProduceBatchRequest.builder()
            .setEntries(
                Collections.singletonList(
                    ProduceBatchRequestEntry.builder()
                        .setId(new TextNode("1"))
                        .setKey(
                            ProduceRequestData.builder()
                                .setFormat(EmbeddedFormat.JSON)
                                .setData(TextNode.valueOf(key))
                                .build())
                        .setValue(
                            ProduceRequestData.builder()
                                .setFormat(EmbeddedFormat.JSON)
                                .setData(TextNode.valueOf(value))
                                .build())
                        // 0 value here is meaningless and only set as originalSize is mandatory for
                        // AutoValue. Value set here is ignored anyway, as "true" originalSize is
                        // calculated & set, when the JSON request is deserialized into a
                        // ProduceRecord object on the server.
                        .setOriginalSize(0L)
                        .build()))
            .build();

    Response response =
        testEnv
            .kafkaRest()
            .target()
            .path("/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME + "/records:batch")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(request, MediaType.APPLICATION_JSON));
    assertEquals(207, response.getStatus());

    List<ProduceBatchResponseFailureEntry> actual = readResponseFailures(response);
    assertEquals(1, actual.size());
    // Check request was rate-limited, so return http error-code is 429.
    // NOTE - Byte rate-limit is set as 1 in setup() making sure 1st request fails.
    assertEquals(429, actual.get(0).getErrorCode());
  }

  @DisplayName("test_whenGlobalByteLimitReached_thenCallerIsRateLimited")
  @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
  @ValueSource(strings = {"kraft", "zk"})
  public void test_whenGlobalByteLimitReached_thenCallerIsRateLimited(String quorum)
      throws Exception {
    doByteLimitReachedTest();
  }

  @DisplayName("test_whenClusterByteLimitReached_thenCallerIsRateLimited")
  @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
  @ValueSource(strings = {"kraft", "zk"})
  public void test_whenClusterByteLimitReached_thenCallerIsRateLimited(String quorum)
      throws Exception {
    doByteLimitReachedTest();
  }

  private void doCountLimitTest() throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    String key = "foo";
    String value = "bar";
    ProduceBatchRequest request =
        ProduceBatchRequest.builder()
            .setEntries(
                Collections.singletonList(
                    ProduceBatchRequestEntry.builder()
                        .setId(new TextNode("1"))
                        .setKey(
                            ProduceRequestData.builder()
                                .setFormat(EmbeddedFormat.JSON)
                                .setData(TextNode.valueOf(key))
                                .build())
                        .setValue(
                            ProduceRequestData.builder()
                                .setFormat(EmbeddedFormat.JSON)
                                .setData(TextNode.valueOf(value))
                                .build())
                        .setOriginalSize(0L)
                        .build()))
            .build();

    Response response1 =
        testEnv
            .kafkaRest()
            .target()
            .path("/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME + "/records:batch")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(request, MediaType.APPLICATION_JSON));

    Response response2 =
        testEnv
            .kafkaRest()
            .target()
            .path("/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME + "/records:batch")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(request, MediaType.APPLICATION_JSON));

    assertEquals(207, response1.getStatus());
    List<ProduceBatchResponseSuccessEntry> successes = readResponseSuccesses(response1);
    assertEquals(1, successes.size());

    assertEquals(207, response2.getStatus());
    List<ProduceBatchResponseFailureEntry> failures = readResponseFailures(response2);
    assertEquals(1, failures.size());
    // Check request was rate-limited, so return http error-code is 429.
    // NOTE - Count rate-limit is set as 1 in setup() making sure 2nd request fails
    // deterministically.
    assertEquals(429, failures.get(0).getErrorCode());
  }

  @DisplayName("test_whenGlobalRequestCountLimitReached_thenCallerIsRateLimited")
  @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
  @ValueSource(strings = {"kraft", "zk"})
  public void test_whenGlobalRequestCountLimitReached_thenCallerIsRateLimited(String quorum)
      throws Exception {
    doCountLimitTest();
  }

  @DisplayName("test_whenClusterRequestCountLimitReached_thenCallerIsRateLimited")
  @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
  @ValueSource(strings = {"kraft", "zk"})
  public void test_whenClusterRequestCountLimitReached_thenCallerIsRateLimited(String quorum)
      throws Exception {
    doCountLimitTest();
  }

  private static ProduceBatchResponseSuccessEntry readResponseSuccessEntry(
      Response response, String id) {
    response.bufferEntity();
    ProduceBatchResponse responseEntity = response.readEntity(ProduceBatchResponse.class);
    return responseEntity.getSuccesses().stream()
        .filter((e) -> e.getId().equals(id))
        .findFirst()
        .orElseThrow(() -> new RuntimeException("Batch response does not contain id"));
  }

  private static ProduceBatchResponseFailureEntry readResponseFailureEntry(
      Response response, String id) {
    response.bufferEntity();
    ProduceBatchResponse responseEntity = response.readEntity(ProduceBatchResponse.class);
    return responseEntity.getFailures().stream()
        .filter((e) -> e.getId().equals(id))
        .findFirst()
        .orElseThrow(() -> new RuntimeException("Batch response does not contain id"));
  }

  private static ImmutableList<ProduceBatchResponseSuccessEntry> readResponseSuccesses(
      Response response) {
    return response.readEntity(ProduceBatchResponse.class).getSuccesses();
  }

  private static ImmutableList<ProduceBatchResponseFailureEntry> readResponseFailures(
      Response response) {
    return response.readEntity(ProduceBatchResponse.class).getFailures();
  }

  private static byte[] generateBinaryData(int messageSize) {
    byte[] data = new byte[messageSize];
    Arrays.fill(data, (byte) 1);
    return data;
  }
}
