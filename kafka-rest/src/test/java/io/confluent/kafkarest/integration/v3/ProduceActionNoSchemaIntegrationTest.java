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
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.BinaryNode;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import io.confluent.kafka.serializers.KafkaJsonDeserializer;
import io.confluent.kafkarest.entities.EmbeddedFormat;
import io.confluent.kafkarest.entities.v3.ProduceRequest;
import io.confluent.kafkarest.entities.v3.ProduceRequest.ProduceRequestData;
import io.confluent.kafkarest.entities.v3.ProduceRequest.ProduceRequestHeader;
import io.confluent.kafkarest.entities.v3.ProduceResponse;
import io.confluent.kafkarest.exceptions.v3.ErrorResponse;
import io.confluent.kafkarest.integration.ClusterTestHarness;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import javax.ws.rs.ProcessingException;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class ProduceActionNoSchemaIntegrationTest extends ClusterTestHarness {

  private static final String TOPIC_NAME = "topic-1";
  private static final int NUM_PARTITIONS = 3;

  public ProduceActionNoSchemaIntegrationTest() {
    super(/* numBrokers= */ 1, /* withSchemaRegistry= */ false);
  }

  @BeforeEach
  @Override
  public void setUp(TestInfo testInfo) throws Exception {
    super.setUp(testInfo);

    createTopic(TOPIC_NAME, NUM_PARTITIONS, (short) 1);
  }

  @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
  @ValueSource(strings = {"kraft", "zk"})
  public void produceBinary(String quorum) throws Exception {
    String clusterId = getClusterId();
    ByteString key = ByteString.copyFromUtf8("foo");
    ByteString value = ByteString.copyFromUtf8("bar");
    ProduceRequest request =
        ProduceRequest.builder()
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
            .build();

    Response response =
        request("/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME + "/records")
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(request, MediaType.APPLICATION_JSON));
    assertEquals(Status.OK.getStatusCode(), response.getStatus());

    ProduceResponse actual = readProduceResponse(response);
    ConsumerRecord<byte[], byte[]> produced =
        getMessage(
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
    String clusterId = getClusterId();
    ProduceRequest request =
        ProduceRequest.builder()
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
            .build();

    Response response =
        request("/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME + "/records")
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(request, MediaType.APPLICATION_JSON));
    assertEquals(Status.OK.getStatusCode(), response.getStatus());

    ProduceResponse actual = readProduceResponse(response);
    ConsumerRecord<byte[], byte[]> produced =
        getMessage(
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
    String clusterId = getClusterId();
    ProduceRequest request =
        ProduceRequest.builder()
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
            .build();

    Response response =
        request("/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME + "/records")
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(request, MediaType.APPLICATION_JSON));
    assertEquals(Status.OK.getStatusCode(), response.getStatus());

    ErrorResponse actual = response.readEntity(ErrorResponse.class);
    assertEquals(400, actual.getErrorCode());
  }

  @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
  @ValueSource(strings = {"kraft", "zk"})
  public void produceString(String quorum) throws Exception {
    String clusterId = getClusterId();
    String key = "foo";
    String value = "bar";
    ProduceRequest request =
        ProduceRequest.builder()
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
            .build();

    Response response =
        request("/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME + "/records")
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(request, MediaType.APPLICATION_JSON));
    assertEquals(Status.OK.getStatusCode(), response.getStatus());

    ProduceResponse actual = readProduceResponse(response);
    ConsumerRecord<String, String> produced =
        getMessage(
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
  public void produceStringCharsetUtf8(String quorum) throws Exception {
    String clusterId = getClusterId();
    String key = "foo";
    String value = "bar";
    ProduceRequest request =
        ProduceRequest.builder()
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
            .build();

    Response response =
        request("/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME + "/records")
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(request, MediaType.APPLICATION_JSON + "; charset=UTF-8"));
    assertEquals(Status.OK.getStatusCode(), response.getStatus());

    ProduceResponse actual = readProduceResponse(response);
    ConsumerRecord<String, String> produced =
        getMessage(
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
  public void produceStringCharsetIso88591(String quorum) throws Exception {
    String clusterId = getClusterId();
    String key = "foo";
    String value = "bar";
    ProduceRequest request =
        ProduceRequest.builder()
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
            .build();

    Response response =
        request("/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME + "/records")
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(request, MediaType.APPLICATION_JSON + "; charset=ISO-8859-1"));
    assertEquals(Status.OK.getStatusCode(), response.getStatus());

    ProduceResponse actual = readProduceResponse(response);
    ConsumerRecord<String, String> produced =
        getMessage(
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
  public void produceStringCharsetInvalid(String quorum) throws Exception {
    String clusterId = getClusterId();
    String key = "foo";
    String value = "bar";
    ProduceRequest request =
        ProduceRequest.builder()
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
            .build();

    Response response =
        request("/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME + "/records")
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(request, MediaType.APPLICATION_JSON + "; charset=DEADBEEF"));
    assertEquals(Status.BAD_REQUEST.getStatusCode(), response.getStatus());
  }

  @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
  @ValueSource(strings = {"kraft", "zk"})
  public void produceStringWithEmptyData(String quorum) throws Exception {
    String clusterId = getClusterId();
    ProduceRequest request =
        ProduceRequest.builder()
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
            .build();

    Response response =
        request("/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME + "/records")
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(request, MediaType.APPLICATION_JSON));
    assertEquals(Status.OK.getStatusCode(), response.getStatus());

    ProduceResponse actual = readProduceResponse(response);
    ConsumerRecord<String, String> produced =
        getMessage(
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
    String clusterId = getClusterId();
    ProduceRequest request =
        ProduceRequest.builder()
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
            .build();

    Response response =
        request("/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME + "/records")
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(request, MediaType.APPLICATION_JSON));
    assertEquals(Status.OK.getStatusCode(), response.getStatus());

    ProduceResponse actual = readProduceResponse(response);
    ConsumerRecord<String, String> produced =
        getMessage(
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
    String clusterId = getClusterId();
    String request = "{ \"records\": {\"subject\": \"foobar\" } }";

    Response response =
        request("/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME + "/records")
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(request, MediaType.APPLICATION_JSON));
    assertEquals(Status.OK.getStatusCode(), response.getStatus());
    ErrorResponse actual = response.readEntity(ErrorResponse.class);
    assertEquals(400, actual.getErrorCode());
    assertEquals(
        "Unrecognized field \"records\" "
            + "(class io.confluent.kafkarest.entities.v3.AutoValue_ProduceRequest$Builder), "
            + "not marked as ignorable (6 known properties: \"value\", \"originalSize\", "
            + "\"partitionId\", \"headers\", \"key\", \"timestamp\"])",
        actual.getMessage());
  }

  @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
  @ValueSource(strings = {"kraft", "zk"})
  public void produceJson(String quorum) throws Exception {
    String clusterId = getClusterId();
    String key = "foo";
    String value = "bar";
    ProduceRequest request =
        ProduceRequest.builder()
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
            .build();

    Response response =
        request("/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME + "/records")
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(request, MediaType.APPLICATION_JSON));
    assertEquals(Status.OK.getStatusCode(), response.getStatus());

    ProduceResponse actual = readProduceResponse(response);
    KafkaJsonDeserializer<Object> deserializer = new KafkaJsonDeserializer<>();
    deserializer.configure(emptyMap(), /* isKey= */ false);
    ConsumerRecord<Object, Object> produced =
        getMessage(
            TOPIC_NAME, actual.getPartitionId(), actual.getOffset(), deserializer, deserializer);
    assertEquals(key, produced.key());
    assertEquals(value, produced.value());
  }

  @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
  @ValueSource(strings = {"kraft", "zk"})
  public void produceJsonWithNullData(String quorum) throws Exception {
    String clusterId = getClusterId();
    ProduceRequest request =
        ProduceRequest.builder()
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
            .build();

    Response response =
        request("/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME + "/records")
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(request, MediaType.APPLICATION_JSON));
    assertEquals(Status.OK.getStatusCode(), response.getStatus());

    ProduceResponse actual = readProduceResponse(response);
    KafkaJsonDeserializer<Object> deserializer = new KafkaJsonDeserializer<>();
    deserializer.configure(emptyMap(), /* isKey= */ false);
    ConsumerRecord<Object, Object> produced =
        getMessage(
            TOPIC_NAME, actual.getPartitionId(), actual.getOffset(), deserializer, deserializer);
    assertNull(produced.key());
    assertNull(produced.value());
  }

  @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
  @ValueSource(strings = {"kraft", "zk"})
  public void produceBinaryWithPartitionId(String quorum) throws Exception {
    String clusterId = getClusterId();
    int partitionId = 1;
    ByteString key = ByteString.copyFromUtf8("foo");
    ByteString value = ByteString.copyFromUtf8("bar");
    ProduceRequest request =
        ProduceRequest.builder()
            .setPartitionId(partitionId)
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
            .build();

    Response response =
        request("/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME + "/records")
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(request, MediaType.APPLICATION_JSON));
    assertEquals(Status.OK.getStatusCode(), response.getStatus());

    ProduceResponse actual = readProduceResponse(response);
    ConsumerRecord<byte[], byte[]> produced =
        getMessage(
            TOPIC_NAME,
            partitionId,
            actual.getOffset(),
            new ByteArrayDeserializer(),
            new ByteArrayDeserializer());
    assertEquals(key, ByteString.copyFrom(produced.key()));
    assertEquals(value, ByteString.copyFrom(produced.value()));
  }

  @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
  @ValueSource(strings = {"kraft", "zk"})
  public void produceBinaryWithTimestamp(String quorum) throws Exception {
    String clusterId = getClusterId();
    Instant timestamp = Instant.ofEpochMilli(1000);
    ByteString key = ByteString.copyFromUtf8("foo");
    ByteString value = ByteString.copyFromUtf8("bar");
    ProduceRequest request =
        ProduceRequest.builder()
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
            .setTimestamp(timestamp)
            .setOriginalSize(0L)
            .build();

    Response response =
        request("/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME + "/records")
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(request, MediaType.APPLICATION_JSON));
    assertEquals(Status.OK.getStatusCode(), response.getStatus());

    ProduceResponse actual = readProduceResponse(response);
    ConsumerRecord<byte[], byte[]> produced =
        getMessage(
            TOPIC_NAME,
            actual.getPartitionId(),
            actual.getOffset(),
            new ByteArrayDeserializer(),
            new ByteArrayDeserializer());
    assertEquals(key, ByteString.copyFrom(produced.key()));
    assertEquals(value, ByteString.copyFrom(produced.value()));
    assertEquals(timestamp, Instant.ofEpochMilli(produced.timestamp()));
  }

  @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
  @ValueSource(strings = {"kraft", "zk"})
  public void produceBinaryWithHeaders(String quorum) throws Exception {
    String clusterId = getClusterId();
    ByteString key = ByteString.copyFromUtf8("foo");
    ByteString value = ByteString.copyFromUtf8("bar");
    ProduceRequest request =
        ProduceRequest.builder()
            .setHeaders(
                Arrays.asList(
                    ProduceRequestHeader.create("header-1", ByteString.copyFromUtf8("value-1")),
                    ProduceRequestHeader.create("header-1", ByteString.copyFromUtf8("value-2")),
                    ProduceRequestHeader.create("header-2", ByteString.copyFromUtf8("value-3"))))
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
            .build();

    Response response =
        request("/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME + "/records")
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(request, MediaType.APPLICATION_JSON));
    assertEquals(Status.OK.getStatusCode(), response.getStatus());

    ProduceResponse actual = readProduceResponse(response);
    ConsumerRecord<byte[], byte[]> produced =
        getMessage(
            TOPIC_NAME,
            actual.getPartitionId(),
            actual.getOffset(),
            new ByteArrayDeserializer(),
            new ByteArrayDeserializer());
    assertEquals(key, ByteString.copyFrom(produced.key()));
    assertEquals(value, ByteString.copyFrom(produced.value()));
    assertEquals(
        Arrays.asList(
            new RecordHeader("header-1", ByteString.copyFromUtf8("value-1").toByteArray()),
            new RecordHeader("header-1", ByteString.copyFromUtf8("value-2").toByteArray())),
        ImmutableList.copyOf(produced.headers().headers("header-1")));
    assertEquals(
        singletonList(
            new RecordHeader("header-2", ByteString.copyFromUtf8("value-3").toByteArray())),
        ImmutableList.copyOf(produced.headers().headers("header-2")));
  }

  @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
  @ValueSource(strings = {"kraft", "zk"})
  public void produceBinaryKeyOnly(String quorum) throws Exception {
    String clusterId = getClusterId();
    ByteString key = ByteString.copyFromUtf8("foo");
    ProduceRequest request =
        ProduceRequest.builder()
            .setKey(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.BINARY)
                    .setData(BinaryNode.valueOf(key.toByteArray()))
                    .build())
            .setOriginalSize(0L)
            .build();

    Response response =
        request("/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME + "/records")
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(request, MediaType.APPLICATION_JSON));
    assertEquals(Status.OK.getStatusCode(), response.getStatus());

    ProduceResponse actual = readProduceResponse(response);
    ConsumerRecord<byte[], byte[]> produced =
        getMessage(
            TOPIC_NAME,
            actual.getPartitionId(),
            actual.getOffset(),
            new ByteArrayDeserializer(),
            new ByteArrayDeserializer());
    assertEquals(key, ByteString.copyFrom(produced.key()));
    assertNull(produced.value());
  }

  @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
  @ValueSource(strings = {"kraft", "zk"})
  public void produceBinaryValueOnly(String quorum) throws Exception {
    String clusterId = getClusterId();
    ByteString value = ByteString.copyFromUtf8("bar");
    ProduceRequest request =
        ProduceRequest.builder()
            .setValue(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.BINARY)
                    .setData(BinaryNode.valueOf(value.toByteArray()))
                    .build())
            .setOriginalSize(0L)
            .build();

    Response response =
        request("/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME + "/records")
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(request, MediaType.APPLICATION_JSON));
    assertEquals(Status.OK.getStatusCode(), response.getStatus());

    ProduceResponse actual = readProduceResponse(response);
    ConsumerRecord<byte[], byte[]> produced =
        getMessage(
            TOPIC_NAME,
            actual.getPartitionId(),
            actual.getOffset(),
            new ByteArrayDeserializer(),
            new ByteArrayDeserializer());
    assertNull(produced.key());
    assertEquals(value, ByteString.copyFrom(produced.value()));
  }

  @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
  @ValueSource(strings = {"kraft", "zk"})
  public void produceStringWithPartitionId(String quorum) throws Exception {
    String clusterId = getClusterId();
    int partitionId = 1;
    String key = "foo";
    String value = "bar";
    ProduceRequest request =
        ProduceRequest.builder()
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
            .build();

    Response response =
        request("/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME + "/records")
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(request, MediaType.APPLICATION_JSON));
    assertEquals(Status.OK.getStatusCode(), response.getStatus());

    ProduceResponse actual = readProduceResponse(response);
    ConsumerRecord<String, String> produced =
        getMessage(
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
    String clusterId = getClusterId();
    Instant timestamp = Instant.ofEpochMilli(1000);
    String key = "foo";
    String value = "bar";
    ProduceRequest request =
        ProduceRequest.builder()
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
            .build();

    Response response =
        request("/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME + "/records")
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(request, MediaType.APPLICATION_JSON));
    assertEquals(Status.OK.getStatusCode(), response.getStatus());

    ProduceResponse actual = readProduceResponse(response);
    ConsumerRecord<String, String> produced =
        getMessage(
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
    String clusterId = getClusterId();
    String key = "foo";
    String value = "bar";
    ProduceRequest request =
        ProduceRequest.builder()
            .setHeaders(
                Arrays.asList(
                    ProduceRequestHeader.create("header-1", ByteString.copyFromUtf8("value-1")),
                    ProduceRequestHeader.create("header-1", ByteString.copyFromUtf8("value-2")),
                    ProduceRequestHeader.create("header-2", ByteString.copyFromUtf8("value-3"))))
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
            .build();

    Response response =
        request("/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME + "/records")
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(request, MediaType.APPLICATION_JSON));
    assertEquals(Status.OK.getStatusCode(), response.getStatus());

    ProduceResponse actual = readProduceResponse(response);
    ConsumerRecord<String, String> produced =
        getMessage(
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
        singletonList(
            new RecordHeader("header-2", ByteString.copyFromUtf8("value-3").toByteArray())),
        ImmutableList.copyOf(produced.headers().headers("header-2")));
  }

  @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
  @ValueSource(strings = {"kraft", "zk"})
  public void produceStringKeyOnly(String quorum) throws Exception {
    String clusterId = getClusterId();
    String key = "foo";
    ProduceRequest request =
        ProduceRequest.builder()
            .setKey(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.STRING)
                    .setData(TextNode.valueOf(key))
                    .build())
            .setOriginalSize(0L)
            .build();

    Response response =
        request("/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME + "/records")
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(request, MediaType.APPLICATION_JSON));
    assertEquals(Status.OK.getStatusCode(), response.getStatus());

    ProduceResponse actual = readProduceResponse(response);
    ConsumerRecord<String, byte[]> produced =
        getMessage(
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
    String clusterId = getClusterId();
    String value = "bar";
    ProduceRequest request =
        ProduceRequest.builder()
            .setValue(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.STRING)
                    .setData(TextNode.valueOf(value))
                    .build())
            .setOriginalSize(0L)
            .build();

    Response response =
        request("/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME + "/records")
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(request, MediaType.APPLICATION_JSON));
    assertEquals(Status.OK.getStatusCode(), response.getStatus());

    ProduceResponse actual = readProduceResponse(response);
    ConsumerRecord<String, String> produced =
        getMessage(
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
    String clusterId = getClusterId();
    ProduceRequest request = ProduceRequest.builder().setOriginalSize(0L).build();

    Response response =
        request("/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME + "/records")
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(request, MediaType.APPLICATION_JSON));
    assertEquals(Status.OK.getStatusCode(), response.getStatus());

    ProduceResponse actual = readProduceResponse(response);
    ConsumerRecord<byte[], byte[]> produced =
        getMessage(
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
    String clusterId = getClusterId();
    ArrayList<ProduceRequest> requests = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      requests.add(
          ProduceRequest.builder()
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
              .build());
    }

    StringBuilder batch = new StringBuilder();
    ObjectMapper objectMapper = getObjectMapper();
    for (ProduceRequest produceRequest : requests) {
      batch.append(objectMapper.writeValueAsString(produceRequest));
    }

    Response response =
        request("/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME + "/records")
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(batch.toString(), MediaType.APPLICATION_JSON));
    assertEquals(Status.OK.getStatusCode(), response.getStatus());

    List<ProduceResponse> actual = readProduceResponses(response);
    KafkaJsonDeserializer<Object> deserializer = new KafkaJsonDeserializer<>();
    deserializer.configure(emptyMap(), /* isKey= */ false);

    ConsumerRecords<Object, Object> producedRecords =
        getMessages(TOPIC_NAME, deserializer, deserializer, 100);

    Iterator<ConsumerRecord<Object, Object>> it = producedRecords.iterator();
    assertEquals(100, producedRecords.count());

    for (int i = 0; i < 100; i++) {
      ConsumerRecord<Object, Object> record = it.next();
      assertEquals(actual.get(i).getPartitionId(), record.partition());
      assertEquals(actual.get(i).getOffset(), record.offset());
      assertEquals(
          requests
              .get(i)
              .getKey()
              .map(ProduceRequestData::getData)
              .map(JsonNode::asText)
              .orElse(null),
          record.key());
      assertEquals(
          requests
              .get(i)
              .getValue()
              .map(ProduceRequestData::getData)
              .map(JsonNode::asText)
              .orElse(null),
          record.value());
    }
  }

  @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
  @ValueSource(strings = {"kraft", "zk"})
  public void produceStringBatch(String quorum) throws Exception {
    String clusterId = getClusterId();
    ArrayList<ProduceRequest> requests = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      requests.add(
          ProduceRequest.builder()
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
              .build());
    }

    StringBuilder batch = new StringBuilder();
    ObjectMapper objectMapper = getObjectMapper();
    for (ProduceRequest produceRequest : requests) {
      batch.append(objectMapper.writeValueAsString(produceRequest));
    }

    Response response =
        request("/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME + "/records")
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(batch.toString(), MediaType.APPLICATION_JSON));
    assertEquals(Status.OK.getStatusCode(), response.getStatus());

    List<ProduceResponse> actual = readProduceResponses(response);
    StringDeserializer deserializer = new StringDeserializer();

    ConsumerRecords<String, String> producedRecords =
        getMessages(TOPIC_NAME, deserializer, deserializer, 100);

    Iterator<ConsumerRecord<String, String>> it = producedRecords.iterator();
    assertEquals(100, producedRecords.count());

    for (int i = 0; i < 100; i++) {
      ConsumerRecord<String, String> record = it.next();
      assertEquals(actual.get(i).getPartitionId(), record.partition());
      assertEquals(actual.get(i).getOffset(), record.offset());
      assertEquals(
          requests
              .get(i)
              .getKey()
              .map(ProduceRequestData::getData)
              .map(JsonNode::textValue)
              .orElse(null),
          record.key());
      assertEquals(
          requests
              .get(i)
              .getValue()
              .map(ProduceRequestData::getData)
              .map(JsonNode::textValue)
              .orElse(null),
          record.value());
    }
  }

  @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
  @ValueSource(strings = {"kraft", "zk"})
  public void produceBinaryBatchWithInvalidData_throwsMultipleBadRequests(String quorum)
      throws Exception {
    String clusterId = getClusterId();
    ArrayList<ProduceRequest> requests = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      requests.add(
          ProduceRequest.builder()
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
              .build());
    }

    StringBuilder batch = new StringBuilder();
    ObjectMapper objectMapper = getObjectMapper();
    for (ProduceRequest produceRequest : requests) {
      batch.append(objectMapper.writeValueAsString(produceRequest));
    }

    Response response =
        request("/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME + "/records")
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(batch.toString(), MediaType.APPLICATION_JSON));
    assertEquals(Status.OK.getStatusCode(), response.getStatus());

    List<ErrorResponse> actual = readErrorResponses(response);
    for (int i = 0; i < 100; i++) {
      assertEquals(400, actual.get(i).getErrorCode());
    }
  }

  @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
  @ValueSource(strings = {"kraft", "zk"})
  public void produceBinaryWithLargerSizeMessage(String quorum) throws Exception {
    String clusterId = getClusterId();
    ByteString key = ByteString.copyFromUtf8("foo");
    // Kafka server and producer is configured to accept messages upto 20971520 Bytes (20MB) but
    // KafkaProducer calculates produced bytes including key, value, headers size and additional
    // record overhead bytes hence producing message of 20971420 bytes.
    int valueSize = ((2 << 20) * 10) - 100;
    byte[] value = generateBinaryData(valueSize);
    ProduceRequest request =
        ProduceRequest.builder()
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
            .build();

    Response response =
        request("/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME + "/records")
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(request, MediaType.APPLICATION_JSON));
    assertEquals(Status.OK.getStatusCode(), response.getStatus());

    ProduceResponse actual = readProduceResponse(response);
    assertTrue(actual.getValue().isPresent());
    assertEquals(valueSize, actual.getValue().get().getSize());

    ConsumerRecord<byte[], byte[]> produced =
        getMessage(
            TOPIC_NAME,
            actual.getPartitionId(),
            actual.getOffset(),
            new ByteArrayDeserializer(),
            new ByteArrayDeserializer());
    assertEquals(key, ByteString.copyFrom(produced.key()));
    assertEquals(valueSize, produced.serializedValueSize());
    assertEquals(Arrays.toString(value), Arrays.toString(produced.value()));
  }

  private static ProduceResponse readProduceResponse(Response response) {
    response.bufferEntity();
    try {
      return response.readEntity(ProduceResponse.class);
    } catch (ProcessingException e) {
      throw new RuntimeException(response.readEntity(ErrorResponse.class).toString(), e);
    }
  }

  private static ImmutableList<ProduceResponse> readProduceResponses(Response response) {
    return ImmutableList.copyOf(
        response.readEntity(new GenericType<MappingIterator<ProduceResponse>>() {}));
  }

  private static ImmutableList<ErrorResponse> readErrorResponses(Response response) {
    return ImmutableList.copyOf(
        response.readEntity(new GenericType<MappingIterator<ErrorResponse>>() {}));
  }

  private static byte[] generateBinaryData(int messageSize) {
    byte[] data = new byte[messageSize];
    Arrays.fill(data, (byte) 1);
    return data;
  }
}
