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

package io.confluent.kafkarest.controllers;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS;
import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.USE_LATEST_VERSION;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.FloatNode;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.BaseEncoding;
import com.google.protobuf.ByteString;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.json.JsonSchemaProvider;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;
import io.confluent.kafka.serializers.subject.TopicNameStrategy;
import io.confluent.kafka.serializers.subject.strategy.SubjectNameStrategy;
import io.confluent.kafkarest.entities.EmbeddedFormat;
import io.confluent.kafkarest.entities.RegisteredSchema;
import io.confluent.kafkarest.exceptions.BadRequestException;
import io.confluent.rest.exceptions.RestConstraintViolationException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class RecordSerializerFacadeTest {
  private static final String TOPIC_NAME = "topic-1";
  private static final String SCHEMA_REGISTRY_SCOPE = "sr";
  private static final Map<String, Object> SCHEMA_SERIALIZER_CONFIGS =
      ImmutableMap.of(
          SCHEMA_REGISTRY_URL_CONFIG, "mock://" + SCHEMA_REGISTRY_SCOPE,
          AUTO_REGISTER_SCHEMAS, false,
          USE_LATEST_VERSION, false);
  private static final SubjectNameStrategy SUBJECT_NAME_STRATEGY = new TopicNameStrategy();
  public static final int SCHEMA_VERSION = 1;

  private MockSchemaRegistryClient schemaRegistryClient;
  private RecordSerializer recordSerializer;

  @BeforeEach
  public void setUp() {
    schemaRegistryClient =
        (MockSchemaRegistryClient)
            MockSchemaRegistry.getClientForScope(
                SCHEMA_REGISTRY_SCOPE,
                Arrays.asList(
                    new AvroSchemaProvider(),
                    new JsonSchemaProvider(),
                    new ProtobufSchemaProvider()));
    recordSerializer =
        new RecordSerializerFacade(
            new NoSchemaRecordSerializer(SCHEMA_SERIALIZER_CONFIGS),
            () ->
                new SchemaRecordSerializerImpl(
                    schemaRegistryClient,
                    SCHEMA_SERIALIZER_CONFIGS,
                    SCHEMA_SERIALIZER_CONFIGS,
                    SCHEMA_SERIALIZER_CONFIGS));
  }

  @Test
  public void noSchemaRegistryClientConfigured() {

    RecordSerializerFacade myRecordSerializer =
        new RecordSerializerFacade(
            new NoSchemaRecordSerializer(SCHEMA_SERIALIZER_CONFIGS),
            () -> new SchemaRecordSerializerThrowing());

    RestConstraintViolationException rcve =
        assertThrows(
            RestConstraintViolationException.class,
            () ->
                myRecordSerializer.serialize(
                    EmbeddedFormat.AVRO,
                    TOPIC_NAME,
                    /* schema= */ Optional.empty(),
                    TextNode.valueOf(
                        BaseEncoding.base64().encode("foobar".getBytes(StandardCharsets.UTF_8))),
                    /* isKey= */ true));

    assertEquals(
        "Error serializing message. Schema Registry not defined, no Schema Registry client available to serialize message.",
        rcve.getMessage());
    assertEquals(42207, rcve.getErrorCode());
  }

  @Test
  public void serializeBinaryKey_returnsSerialized() {
    ByteString serialized =
        recordSerializer
            .serialize(
                EmbeddedFormat.BINARY,
                TOPIC_NAME,
                /* schema= */ Optional.empty(),
                TextNode.valueOf(
                    BaseEncoding.base64().encode("foobar".getBytes(StandardCharsets.UTF_8))),
                /* isKey= */ true)
            .get();

    assertEquals("foobar", serialized.toStringUtf8());
  }

  @Test
  public void serializeBinaryValue_returnsSerialized() {
    ByteString serialized =
        recordSerializer
            .serialize(
                EmbeddedFormat.BINARY,
                TOPIC_NAME,
                /* schema= */ Optional.empty(),
                TextNode.valueOf(
                    BaseEncoding.base64().encode("foobar".getBytes(StandardCharsets.UTF_8))),
                /* isKey= */ false)
            .get();

    assertEquals("foobar", serialized.toStringUtf8());
  }

  @Test
  public void serializeNullBinaryKey_returnsEmpty() {
    Optional<ByteString> serialized =
        recordSerializer.serialize(
            EmbeddedFormat.BINARY,
            TOPIC_NAME,
            /* schema= */ Optional.empty(),
            NullNode.getInstance(),
            /* isKey= */ true);

    assertFalse(serialized.isPresent());
  }

  @Test
  public void serializeNullBinaryValue_returnsEmpty() {
    Optional<ByteString> serialized =
        recordSerializer.serialize(
            EmbeddedFormat.BINARY,
            TOPIC_NAME,
            /* schema= */ Optional.empty(),
            NullNode.getInstance(),
            /* isKey= */ false);

    assertFalse(serialized.isPresent());
  }

  @Test
  public void serializeInvalidBinaryKey_throwsBadRequestException() {
    assertThrows(
        BadRequestException.class,
        () ->
            recordSerializer.serialize(
                EmbeddedFormat.BINARY,
                TOPIC_NAME,
                /* schema= */ Optional.empty(),
                TextNode.valueOf("fooba"),
                /* isKey= */ true));
  }

  @Test
  public void serializeInvalidBinaryValue_throwsBadRequestException() {
    assertThrows(
        BadRequestException.class,
        () ->
            recordSerializer.serialize(
                EmbeddedFormat.BINARY,
                TOPIC_NAME,
                /* schema= */ Optional.empty(),
                TextNode.valueOf("fooba"),
                /* isKey= */ false));
  }

  @Test
  public void serializeStringJsonKey_returnsSerialized() {
    ByteString serialized =
        recordSerializer
            .serialize(
                EmbeddedFormat.JSON,
                TOPIC_NAME,
                /* schema= */ Optional.empty(),
                TextNode.valueOf("foobar"),
                /* isKey= */ true)
            .get();

    assertEquals("\"foobar\"", serialized.toStringUtf8());
  }

  @Test
  public void serializeIntJsonKey_returnsSerialized() {
    ByteString serialized =
        recordSerializer
            .serialize(
                EmbeddedFormat.JSON,
                TOPIC_NAME,
                /* schema= */ Optional.empty(),
                IntNode.valueOf(123),
                /* isKey= */ true)
            .get();

    assertEquals("123", serialized.toStringUtf8());
  }

  @Test
  public void serializeFloatJsonKey_returnsSerialized() {
    ByteString serialized =
        recordSerializer
            .serialize(
                EmbeddedFormat.JSON,
                TOPIC_NAME,
                /* schema= */ Optional.empty(),
                FloatNode.valueOf(123.456F),
                /* isKey= */ true)
            .get();

    assertEquals("123.456", serialized.toStringUtf8());
  }

  @Test
  public void serializeBooleanJsonKey_returnsSerialized() {
    ByteString serialized =
        recordSerializer
            .serialize(
                EmbeddedFormat.JSON,
                TOPIC_NAME,
                /* schema= */ Optional.empty(),
                BooleanNode.valueOf(true),
                /* isKey= */ true)
            .get();

    assertEquals("true", serialized.toStringUtf8());
  }

  @Test
  public void serializeObjectJsonKey_returnsSerialized() {
    ObjectNode node = new ObjectNode(JsonNodeFactory.instance);
    node.put("foo", 1);
    node.put("bar", false);

    ByteString serialized =
        recordSerializer
            .serialize(
                EmbeddedFormat.JSON,
                TOPIC_NAME,
                /* schema= */ Optional.empty(),
                node,
                /* isKey= */ true)
            .get();

    assertEquals("{\"foo\":1,\"bar\":false}", serialized.toStringUtf8());
  }

  @Test
  public void serializeArrayJsonKey_returnsSerialized() {
    ArrayNode node = new ArrayNode(JsonNodeFactory.instance);
    node.add(123.456F);
    node.add(NullNode.getInstance());

    ByteString serialized =
        recordSerializer
            .serialize(
                EmbeddedFormat.JSON,
                TOPIC_NAME,
                /* schema= */ Optional.empty(),
                node,
                /* isKey= */ true)
            .get();

    assertEquals("[123.456,null]", serialized.toStringUtf8());
  }

  @Test
  public void serializeNullJsonKey_returnsEmpty() {
    Optional<ByteString> serialized =
        recordSerializer.serialize(
            EmbeddedFormat.JSON,
            TOPIC_NAME,
            /* schema= */ Optional.empty(),
            NullNode.getInstance(),
            /* isKey= */ true);

    assertFalse(serialized.isPresent());
  }

  @Test
  public void serializeStringJsonValue_returnsSerialized() {
    ByteString serialized =
        recordSerializer
            .serialize(
                EmbeddedFormat.JSON,
                TOPIC_NAME,
                /* schema= */ Optional.empty(),
                TextNode.valueOf("foobar"),
                /* isKey= */ false)
            .get();

    assertEquals("\"foobar\"", serialized.toStringUtf8());
  }

  @Test
  public void serializeIntJsonValue_returnsSerialized() {
    ByteString serialized =
        recordSerializer
            .serialize(
                EmbeddedFormat.JSON,
                TOPIC_NAME,
                /* schema= */ Optional.empty(),
                IntNode.valueOf(123),
                /* isKey= */ false)
            .get();

    assertEquals("123", serialized.toStringUtf8());
  }

  @Test
  public void serializeFloatJsonValue_returnsSerialized() {
    ByteString serialized =
        recordSerializer
            .serialize(
                EmbeddedFormat.JSON,
                TOPIC_NAME,
                /* schema= */ Optional.empty(),
                FloatNode.valueOf(123.456F),
                /* isKey= */ false)
            .get();

    assertEquals("123.456", serialized.toStringUtf8());
  }

  @Test
  public void serializeBooleanJsonValue_returnsSerialized() {
    ByteString serialized =
        recordSerializer
            .serialize(
                EmbeddedFormat.JSON,
                TOPIC_NAME,
                /* schema= */ Optional.empty(),
                BooleanNode.valueOf(true),
                /* isKey= */ false)
            .get();

    assertEquals("true", serialized.toStringUtf8());
  }

  @Test
  public void serializeObjectJsonValue_returnsSerialized() {
    ObjectNode node = new ObjectNode(JsonNodeFactory.instance);
    node.put("foo", 1);
    node.put("bar", false);

    ByteString serialized =
        recordSerializer
            .serialize(
                EmbeddedFormat.JSON,
                TOPIC_NAME,
                /* schema= */ Optional.empty(),
                node,
                /* isKey= */ false)
            .get();

    assertEquals("{\"foo\":1,\"bar\":false}", serialized.toStringUtf8());
  }

  @Test
  public void serializeArrayJsonValue_returnsSerialized() {
    ArrayNode node = new ArrayNode(JsonNodeFactory.instance);
    node.add(123.456F);
    node.add(NullNode.getInstance());

    ByteString serialized =
        recordSerializer
            .serialize(
                EmbeddedFormat.JSON,
                TOPIC_NAME,
                /* schema= */ Optional.empty(),
                node,
                /* isKey= */ false)
            .get();

    assertEquals("[123.456,null]", serialized.toStringUtf8());
  }

  @Test
  public void serializeNullJsonValue_returnsEmpty() {
    Optional<ByteString> serialized =
        recordSerializer.serialize(
            EmbeddedFormat.JSON,
            TOPIC_NAME,
            /* schema= */ Optional.empty(),
            NullNode.getInstance(),
            /* isKey= */ false);

    assertFalse(serialized.isPresent());
  }

  @Test
  public void serializeStringAvroKey_returnsSerialized() throws Exception {
    AvroSchema schema = new AvroSchema("{\"type\": \"string\"}");
    String subject = SUBJECT_NAME_STRATEGY.subjectName(TOPIC_NAME, /* isKey= */ true, schema);
    int schemaId = schemaRegistryClient.register(subject, schema);

    ByteString serialized =
        recordSerializer
            .serialize(
                EmbeddedFormat.AVRO,
                TOPIC_NAME,
                Optional.of(RegisteredSchema.create(subject, schemaId, SCHEMA_VERSION, schema)),
                TextNode.valueOf("foobar"),
                /* isKey= */ true)
            .get();

    KafkaAvroDeserializer deserializer = new KafkaAvroDeserializer();
    deserializer.configure(SCHEMA_SERIALIZER_CONFIGS, /* isKey= */ true);
    Object deserialized =
        deserializer.deserialize(TOPIC_NAME, serialized.toByteArray(), schema.rawSchema());
    assertEquals("foobar", deserialized);
  }

  @Test
  public void serializeIntAvroKey_returnsSerialized() throws Exception {
    AvroSchema schema = new AvroSchema("{\"type\": \"int\"}");
    String subject = SUBJECT_NAME_STRATEGY.subjectName(TOPIC_NAME, /* isKey= */ true, schema);
    int schemaId = schemaRegistryClient.register(subject, schema);

    ByteString serialized =
        recordSerializer
            .serialize(
                EmbeddedFormat.AVRO,
                TOPIC_NAME,
                Optional.of(RegisteredSchema.create(subject, schemaId, SCHEMA_VERSION, schema)),
                IntNode.valueOf(123),
                /* isKey= */ true)
            .get();

    KafkaAvroDeserializer deserializer = new KafkaAvroDeserializer();
    deserializer.configure(SCHEMA_SERIALIZER_CONFIGS, /* isKey= */ true);
    Object deserialized =
        deserializer.deserialize(TOPIC_NAME, serialized.toByteArray(), schema.rawSchema());
    assertEquals(123, deserialized);
  }

  @Test
  public void serializeFloatAvroKey_returnsSerialized() throws Exception {
    AvroSchema schema = new AvroSchema("{\"type\": \"float\"}");
    String subject = SUBJECT_NAME_STRATEGY.subjectName(TOPIC_NAME, /* isKey= */ true, schema);
    int schemaId = schemaRegistryClient.register(subject, schema);

    ByteString serialized =
        recordSerializer
            .serialize(
                EmbeddedFormat.AVRO,
                TOPIC_NAME,
                Optional.of(RegisteredSchema.create(subject, schemaId, SCHEMA_VERSION, schema)),
                FloatNode.valueOf(123.456F),
                /* isKey= */ true)
            .get();

    KafkaAvroDeserializer deserializer = new KafkaAvroDeserializer();
    deserializer.configure(SCHEMA_SERIALIZER_CONFIGS, /* isKey= */ true);
    Object deserialized =
        deserializer.deserialize(TOPIC_NAME, serialized.toByteArray(), schema.rawSchema());
    assertEquals(123.456F, deserialized);
  }

  @Test
  public void serializeBooleanAvroKey_returnsSerialized() throws Exception {
    AvroSchema schema = new AvroSchema("{\"type\": \"boolean\"}");
    String subject = SUBJECT_NAME_STRATEGY.subjectName(TOPIC_NAME, /* isKey= */ true, schema);
    int schemaId = schemaRegistryClient.register(subject, schema);

    ByteString serialized =
        recordSerializer
            .serialize(
                EmbeddedFormat.AVRO,
                TOPIC_NAME,
                Optional.of(RegisteredSchema.create(subject, schemaId, SCHEMA_VERSION, schema)),
                BooleanNode.valueOf(true),
                /* isKey= */ true)
            .get();

    KafkaAvroDeserializer deserializer = new KafkaAvroDeserializer();
    deserializer.configure(SCHEMA_SERIALIZER_CONFIGS, /* isKey= */ true);
    Object deserialized =
        deserializer.deserialize(TOPIC_NAME, serialized.toByteArray(), schema.rawSchema());
    assertEquals(true, deserialized);
  }

  @Test
  public void serializeBytesAvroKey_returnsSerialized() throws Exception {
    AvroSchema schema = new AvroSchema("{\"type\": \"bytes\"}");
    String subject = SUBJECT_NAME_STRATEGY.subjectName(TOPIC_NAME, /* isKey= */ true, schema);
    int schemaId = schemaRegistryClient.register(subject, schema);

    ByteString serialized =
        recordSerializer
            .serialize(
                EmbeddedFormat.AVRO,
                TOPIC_NAME,
                Optional.of(RegisteredSchema.create(subject, schemaId, SCHEMA_VERSION, schema)),
                TextNode.valueOf("foobar"),
                /* isKey= */ true)
            .get();

    KafkaAvroDeserializer deserializer = new KafkaAvroDeserializer();
    deserializer.configure(SCHEMA_SERIALIZER_CONFIGS, /* isKey= */ true);
    Object deserialized =
        deserializer.deserialize(TOPIC_NAME, serialized.toByteArray(), schema.rawSchema());
    assertEquals(
        ByteString.copyFrom("foobar", StandardCharsets.ISO_8859_1),
        ByteString.copyFrom((byte[]) deserialized));
  }

  @Test
  public void serializeRecordAvroKey_returnsSerialized() throws Exception {
    AvroSchema schema =
        new AvroSchema(
            "{"
                + "    \"name\": \"foobar\","
                + "    \"type\": \"record\","
                + "    \"fields\": ["
                + "        {\"name\": \"foo\", \"type\": \"int\"},"
                + "        {\"name\": \"bar\", \"type\": \"boolean\"}"
                + "    ]"
                + "}");
    String subject = SUBJECT_NAME_STRATEGY.subjectName(TOPIC_NAME, /* isKey= */ true, schema);
    int schemaId = schemaRegistryClient.register(subject, schema);
    ObjectNode node = new ObjectNode(JsonNodeFactory.instance);
    node.put("foo", 1);
    node.put("bar", false);

    ByteString serialized =
        recordSerializer
            .serialize(
                EmbeddedFormat.AVRO,
                TOPIC_NAME,
                Optional.of(RegisteredSchema.create(subject, schemaId, SCHEMA_VERSION, schema)),
                node,
                /* isKey= */ true)
            .get();

    KafkaAvroDeserializer deserializer = new KafkaAvroDeserializer();
    deserializer.configure(SCHEMA_SERIALIZER_CONFIGS, /* isKey= */ true);
    Object deserialized =
        deserializer.deserialize(TOPIC_NAME, serialized.toByteArray(), schema.rawSchema());
    GenericRecord expected = new GenericData.Record(schema.rawSchema());
    expected.put("foo", 1);
    expected.put("bar", false);
    assertEquals(expected, deserialized);
  }

  @Test
  public void serializeEnumAvroKey_returnsSerialized() throws Exception {
    AvroSchema schema =
        new AvroSchema(
            "{"
                + "    \"name\": \"foobar\","
                + "    \"type\": \"enum\","
                + "    \"symbols\": [\"foo\", \"bar\"]"
                + "}");
    String subject = SUBJECT_NAME_STRATEGY.subjectName(TOPIC_NAME, /* isKey= */ true, schema);
    int schemaId = schemaRegistryClient.register(subject, schema);

    ByteString serialized =
        recordSerializer
            .serialize(
                EmbeddedFormat.AVRO,
                TOPIC_NAME,
                Optional.of(RegisteredSchema.create(subject, schemaId, SCHEMA_VERSION, schema)),
                TextNode.valueOf("foo"),
                /* isKey= */ true)
            .get();

    KafkaAvroDeserializer deserializer = new KafkaAvroDeserializer();
    deserializer.configure(SCHEMA_SERIALIZER_CONFIGS, /* isKey= */ true);
    Object deserialized =
        deserializer.deserialize(TOPIC_NAME, serialized.toByteArray(), schema.rawSchema());
    assertEquals(new GenericData.EnumSymbol(schema.rawSchema(), "foo"), deserialized);
  }

  @Test
  public void serializeArrayAvroKey_returnsSerialized() throws Exception {
    AvroSchema schema = new AvroSchema("{\"type\": \"array\", \"items\": \"int\"}");
    String subject = SUBJECT_NAME_STRATEGY.subjectName(TOPIC_NAME, /* isKey= */ true, schema);
    int schemaId = schemaRegistryClient.register(subject, schema);
    ArrayNode node = new ArrayNode(JsonNodeFactory.instance);
    node.add(1);
    node.add(2);
    node.add(3);

    ByteString serialized =
        recordSerializer
            .serialize(
                EmbeddedFormat.AVRO,
                TOPIC_NAME,
                Optional.of(RegisteredSchema.create(subject, schemaId, SCHEMA_VERSION, schema)),
                node,
                /* isKey= */ true)
            .get();

    KafkaAvroDeserializer deserializer = new KafkaAvroDeserializer();
    deserializer.configure(SCHEMA_SERIALIZER_CONFIGS, /* isKey= */ true);
    Object deserialized =
        deserializer.deserialize(TOPIC_NAME, serialized.toByteArray(), schema.rawSchema());
    assertEquals(new GenericData.Array<>(schema.rawSchema(), Arrays.asList(1, 2, 3)), deserialized);
  }

  @Test
  public void serializeMapAvroKey_returnsSerialized() throws Exception {
    AvroSchema schema = new AvroSchema("{\"type\": \"map\", \"values\": \"int\"}");
    String subject = SUBJECT_NAME_STRATEGY.subjectName(TOPIC_NAME, /* isKey= */ true, schema);
    int schemaId = schemaRegistryClient.register(subject, schema);
    ObjectNode node = new ObjectNode(JsonNodeFactory.instance);
    node.put("foo", 1);
    node.put("bar", 2);

    ByteString serialized =
        recordSerializer
            .serialize(
                EmbeddedFormat.AVRO,
                TOPIC_NAME,
                Optional.of(RegisteredSchema.create(subject, schemaId, SCHEMA_VERSION, schema)),
                node,
                /* isKey= */ true)
            .get();

    KafkaAvroDeserializer deserializer = new KafkaAvroDeserializer();
    deserializer.configure(SCHEMA_SERIALIZER_CONFIGS, /* isKey= */ true);
    Object deserialized =
        deserializer.deserialize(TOPIC_NAME, serialized.toByteArray(), schema.rawSchema());
    HashMap<Utf8, Integer> expected = new HashMap<>();
    expected.put(new Utf8("foo"), 1);
    expected.put(new Utf8("bar"), 2);
    assertEquals(expected, deserialized);
  }

  @Test
  public void serializeNullAvroKeyNullSchema_returnsEmpty() {
    Optional<ByteString> serialized =
        recordSerializer.serialize(
            EmbeddedFormat.AVRO,
            TOPIC_NAME,
            /* schema= */ Optional.empty(),
            NullNode.getInstance(),
            /* isKey= */ true);

    assertFalse(serialized.isPresent());
  }

  @Test
  public void serializeNullAvroKeyNonNullableSchema_returnsEmpty() throws Exception {
    AvroSchema schema = new AvroSchema("{\"type\": \"int\"}");
    String subject = SUBJECT_NAME_STRATEGY.subjectName(TOPIC_NAME, /* isKey= */ true, schema);
    int schemaId = schemaRegistryClient.register(subject, schema);

    Optional<ByteString> serialized =
        recordSerializer.serialize(
            EmbeddedFormat.AVRO,
            TOPIC_NAME,
            Optional.of(RegisteredSchema.create(subject, schemaId, SCHEMA_VERSION, schema)),
            NullNode.getInstance(),
            /* isKey= */ true);

    assertFalse(serialized.isPresent());
  }

  @Test
  public void serializeNonNullAvroKeyNullSchema_throwsSerializationException() {

    assertThrows(
        RestConstraintViolationException.class,
        () ->
            recordSerializer.serialize(
                EmbeddedFormat.AVRO,
                TOPIC_NAME,
                /* schema= */ Optional.empty(),
                TextNode.valueOf("foobar"),
                /* isKey= */ true));
  }

  @Test
  public void serializeInvalidAvroKey_throwsBadRequestException() throws Exception {
    AvroSchema schema = new AvroSchema("{\"type\": \"int\"}");
    String subject = SUBJECT_NAME_STRATEGY.subjectName(TOPIC_NAME, /* isKey= */ true, schema);
    int schemaId = schemaRegistryClient.register(subject, schema);

    assertThrows(
        BadRequestException.class,
        () ->
            recordSerializer.serialize(
                EmbeddedFormat.AVRO,
                TOPIC_NAME,
                Optional.of(RegisteredSchema.create(subject, schemaId, SCHEMA_VERSION, schema)),
                TextNode.valueOf("foobar"),
                /* isKey= */ true));
  }

  @Test
  public void serializeStringAvroValue_returnsSerialized() throws Exception {
    AvroSchema schema = new AvroSchema("{\"type\": \"string\"}");
    String subject = SUBJECT_NAME_STRATEGY.subjectName(TOPIC_NAME, /* isKey= */ false, schema);
    int schemaId = schemaRegistryClient.register(subject, schema);

    ByteString serialized =
        recordSerializer
            .serialize(
                EmbeddedFormat.AVRO,
                TOPIC_NAME,
                Optional.of(RegisteredSchema.create(subject, schemaId, SCHEMA_VERSION, schema)),
                TextNode.valueOf("foobar"),
                /* isKey= */ false)
            .get();

    KafkaAvroDeserializer deserializer = new KafkaAvroDeserializer();
    deserializer.configure(SCHEMA_SERIALIZER_CONFIGS, /* isKey= */ false);
    Object deserialized =
        deserializer.deserialize(TOPIC_NAME, serialized.toByteArray(), schema.rawSchema());
    assertEquals("foobar", deserialized);
  }

  @Test
  public void serializeIntAvroValue_returnsSerialized() throws Exception {
    AvroSchema schema = new AvroSchema("{\"type\": \"int\"}");
    String subject = SUBJECT_NAME_STRATEGY.subjectName(TOPIC_NAME, /* isKey= */ false, schema);
    int schemaId = schemaRegistryClient.register(subject, schema);

    ByteString serialized =
        recordSerializer
            .serialize(
                EmbeddedFormat.AVRO,
                TOPIC_NAME,
                Optional.of(RegisteredSchema.create(subject, schemaId, SCHEMA_VERSION, schema)),
                IntNode.valueOf(123),
                /* isKey= */ false)
            .get();

    KafkaAvroDeserializer deserializer = new KafkaAvroDeserializer();
    deserializer.configure(SCHEMA_SERIALIZER_CONFIGS, /* isKey= */ false);
    Object deserialized =
        deserializer.deserialize(TOPIC_NAME, serialized.toByteArray(), schema.rawSchema());
    assertEquals(123, deserialized);
  }

  @Test
  public void serializeFloatAvroValue_returnsSerialized() throws Exception {
    AvroSchema schema = new AvroSchema("{\"type\": \"float\"}");
    String subject = SUBJECT_NAME_STRATEGY.subjectName(TOPIC_NAME, /* isKey= */ false, schema);
    int schemaId = schemaRegistryClient.register(subject, schema);

    ByteString serialized =
        recordSerializer
            .serialize(
                EmbeddedFormat.AVRO,
                TOPIC_NAME,
                Optional.of(RegisteredSchema.create(subject, schemaId, SCHEMA_VERSION, schema)),
                FloatNode.valueOf(123.456F),
                /* isKey= */ false)
            .get();

    KafkaAvroDeserializer deserializer = new KafkaAvroDeserializer();
    deserializer.configure(SCHEMA_SERIALIZER_CONFIGS, /* isKey= */ false);
    Object deserialized =
        deserializer.deserialize(TOPIC_NAME, serialized.toByteArray(), schema.rawSchema());
    assertEquals(123.456F, deserialized);
  }

  @Test
  public void serializeBooleanAvroValue_returnsSerialized() throws Exception {
    AvroSchema schema = new AvroSchema("{\"type\": \"boolean\"}");
    String subject = SUBJECT_NAME_STRATEGY.subjectName(TOPIC_NAME, /* isKey= */ false, schema);
    int schemaId = schemaRegistryClient.register(subject, schema);

    ByteString serialized =
        recordSerializer
            .serialize(
                EmbeddedFormat.AVRO,
                TOPIC_NAME,
                Optional.of(RegisteredSchema.create(subject, schemaId, SCHEMA_VERSION, schema)),
                BooleanNode.valueOf(true),
                /* isKey= */ false)
            .get();

    KafkaAvroDeserializer deserializer = new KafkaAvroDeserializer();
    deserializer.configure(SCHEMA_SERIALIZER_CONFIGS, /* isKey= */ false);
    Object deserialized =
        deserializer.deserialize(TOPIC_NAME, serialized.toByteArray(), schema.rawSchema());
    assertEquals(true, deserialized);
  }

  @Test
  public void serializeBytesAvroValue_returnsSerialized() throws Exception {
    AvroSchema schema = new AvroSchema("{\"type\": \"bytes\"}");
    String subject = SUBJECT_NAME_STRATEGY.subjectName(TOPIC_NAME, /* isKey= */ false, schema);
    int schemaId = schemaRegistryClient.register(subject, schema);

    ByteString serialized =
        recordSerializer
            .serialize(
                EmbeddedFormat.AVRO,
                TOPIC_NAME,
                Optional.of(RegisteredSchema.create(subject, schemaId, SCHEMA_VERSION, schema)),
                TextNode.valueOf("foobar"),
                /* isKey= */ false)
            .get();

    KafkaAvroDeserializer deserializer = new KafkaAvroDeserializer();
    deserializer.configure(SCHEMA_SERIALIZER_CONFIGS, /* isKey= */ false);
    Object deserialized =
        deserializer.deserialize(TOPIC_NAME, serialized.toByteArray(), schema.rawSchema());
    assertEquals(
        ByteString.copyFrom("foobar", StandardCharsets.ISO_8859_1),
        ByteString.copyFrom((byte[]) deserialized));
  }

  @Test
  public void serializeRecordAvroValue_returnsSerialized() throws Exception {
    AvroSchema schema =
        new AvroSchema(
            "{"
                + "    \"name\": \"foobar\","
                + "    \"type\": \"record\","
                + "    \"fields\": ["
                + "        {\"name\": \"foo\", \"type\": \"int\"},"
                + "        {\"name\": \"bar\", \"type\": \"boolean\"}"
                + "    ]"
                + "}");
    String subject = SUBJECT_NAME_STRATEGY.subjectName(TOPIC_NAME, /* isKey= */ false, schema);
    int schemaId = schemaRegistryClient.register(subject, schema);
    ObjectNode node = new ObjectNode(JsonNodeFactory.instance);
    node.put("foo", 1);
    node.put("bar", false);

    ByteString serialized =
        recordSerializer
            .serialize(
                EmbeddedFormat.AVRO,
                TOPIC_NAME,
                Optional.of(RegisteredSchema.create(subject, schemaId, SCHEMA_VERSION, schema)),
                node,
                /* isKey= */ false)
            .get();

    KafkaAvroDeserializer deserializer = new KafkaAvroDeserializer();
    deserializer.configure(SCHEMA_SERIALIZER_CONFIGS, /* isKey= */ false);
    Object deserialized =
        deserializer.deserialize(TOPIC_NAME, serialized.toByteArray(), schema.rawSchema());
    GenericRecord expected = new GenericData.Record(schema.rawSchema());
    expected.put("foo", 1);
    expected.put("bar", false);
    assertEquals(expected, deserialized);
  }

  @Test
  public void serializeEnumAvroValue_returnsSerialized() throws Exception {
    AvroSchema schema =
        new AvroSchema(
            "{"
                + "    \"name\": \"foobar\","
                + "    \"type\": \"enum\","
                + "    \"symbols\": [\"foo\", \"bar\"]"
                + "}");
    String subject = SUBJECT_NAME_STRATEGY.subjectName(TOPIC_NAME, /* isKey= */ false, schema);
    int schemaId = schemaRegistryClient.register(subject, schema);

    ByteString serialized =
        recordSerializer
            .serialize(
                EmbeddedFormat.AVRO,
                TOPIC_NAME,
                Optional.of(RegisteredSchema.create(subject, schemaId, SCHEMA_VERSION, schema)),
                TextNode.valueOf("foo"),
                /* isKey= */ false)
            .get();

    KafkaAvroDeserializer deserializer = new KafkaAvroDeserializer();
    deserializer.configure(SCHEMA_SERIALIZER_CONFIGS, /* isKey= */ false);
    Object deserialized =
        deserializer.deserialize(TOPIC_NAME, serialized.toByteArray(), schema.rawSchema());
    assertEquals(new GenericData.EnumSymbol(schema.rawSchema(), "foo"), deserialized);
  }

  @Test
  public void serializeArrayAvroValue_returnsSerialized() throws Exception {
    AvroSchema schema = new AvroSchema("{\"type\": \"array\", \"items\": \"int\"}");
    String subject = SUBJECT_NAME_STRATEGY.subjectName(TOPIC_NAME, /* isKey= */ false, schema);
    int schemaId = schemaRegistryClient.register(subject, schema);
    ArrayNode node = new ArrayNode(JsonNodeFactory.instance);
    node.add(1);
    node.add(2);
    node.add(3);

    ByteString serialized =
        recordSerializer
            .serialize(
                EmbeddedFormat.AVRO,
                TOPIC_NAME,
                Optional.of(RegisteredSchema.create(subject, schemaId, SCHEMA_VERSION, schema)),
                node,
                /* isKey= */ false)
            .get();

    KafkaAvroDeserializer deserializer = new KafkaAvroDeserializer();
    deserializer.configure(SCHEMA_SERIALIZER_CONFIGS, /* isKey= */ false);
    Object deserialized =
        deserializer.deserialize(TOPIC_NAME, serialized.toByteArray(), schema.rawSchema());
    assertEquals(new GenericData.Array<>(schema.rawSchema(), Arrays.asList(1, 2, 3)), deserialized);
  }

  @Test
  public void serializeMapAvroValue_returnsSerialized() throws Exception {
    AvroSchema schema = new AvroSchema("{\"type\": \"map\", \"values\": \"int\"}");
    String subject = SUBJECT_NAME_STRATEGY.subjectName(TOPIC_NAME, /* isKey= */ false, schema);
    int schemaId = schemaRegistryClient.register(subject, schema);
    ObjectNode node = new ObjectNode(JsonNodeFactory.instance);
    node.put("foo", 1);
    node.put("bar", 2);

    ByteString serialized =
        recordSerializer
            .serialize(
                EmbeddedFormat.AVRO,
                TOPIC_NAME,
                Optional.of(RegisteredSchema.create(subject, schemaId, SCHEMA_VERSION, schema)),
                node,
                /* isKey= */ false)
            .get();

    KafkaAvroDeserializer deserializer = new KafkaAvroDeserializer();
    deserializer.configure(SCHEMA_SERIALIZER_CONFIGS, /* isKey= */ false);
    Object deserialized =
        deserializer.deserialize(TOPIC_NAME, serialized.toByteArray(), schema.rawSchema());
    HashMap<Utf8, Integer> expected = new HashMap<>();
    expected.put(new Utf8("foo"), 1);
    expected.put(new Utf8("bar"), 2);
    assertEquals(expected, deserialized);
  }

  @Test
  public void serializeNullAvroValueNullSchema_returnsEmpty() {
    Optional<ByteString> serialized =
        recordSerializer.serialize(
            EmbeddedFormat.AVRO,
            TOPIC_NAME,
            /* schema= */ Optional.empty(),
            NullNode.getInstance(),
            /* isKey= */ false);

    assertFalse(serialized.isPresent());
  }

  @Test
  public void serializeNullAvroValueNonNullableSchema_returnsEmpty() throws Exception {
    AvroSchema schema = new AvroSchema("{\"type\": \"int\"}");
    String subject = SUBJECT_NAME_STRATEGY.subjectName(TOPIC_NAME, /* isKey= */ false, schema);
    int schemaId = schemaRegistryClient.register(subject, schema);

    Optional<ByteString> serialized =
        recordSerializer.serialize(
            EmbeddedFormat.AVRO,
            TOPIC_NAME,
            Optional.of(RegisteredSchema.create(subject, schemaId, SCHEMA_VERSION, schema)),
            NullNode.getInstance(),
            /* isKey= */ false);

    assertFalse(serialized.isPresent());
  }

  @Test
  public void serializeNonNullAvroValueNullSchema_throwsSerializationException() {
    assertThrows(
        RestConstraintViolationException.class,
        () ->
            recordSerializer.serialize(
                EmbeddedFormat.AVRO,
                TOPIC_NAME,
                /* schema= */ Optional.empty(),
                TextNode.valueOf("foobar"),
                /* isKey= */ false));
  }

  @Test
  public void serializeInvalidAvroValue_throwsBadRequestException() throws Exception {
    AvroSchema schema = new AvroSchema("{\"type\": \"int\"}");
    String subject = SUBJECT_NAME_STRATEGY.subjectName(TOPIC_NAME, /* isKey= */ false, schema);
    int schemaId = schemaRegistryClient.register(subject, schema);

    assertThrows(
        BadRequestException.class,
        () ->
            recordSerializer.serialize(
                EmbeddedFormat.AVRO,
                TOPIC_NAME,
                Optional.of(RegisteredSchema.create(subject, schemaId, SCHEMA_VERSION, schema)),
                TextNode.valueOf("foobar"),
                /* isKey= */ false));
  }

  @Test
  public void serializeStringJsonschemaKey_returnsSerialized() throws Exception {
    JsonSchema schema = new JsonSchema("{\"type\": \"string\"}");
    String subject = SUBJECT_NAME_STRATEGY.subjectName(TOPIC_NAME, /* isKey= */ true, schema);
    int schemaId = schemaRegistryClient.register(subject, schema);

    ByteString serialized =
        recordSerializer
            .serialize(
                EmbeddedFormat.JSONSCHEMA,
                TOPIC_NAME,
                Optional.of(RegisteredSchema.create(subject, schemaId, SCHEMA_VERSION, schema)),
                TextNode.valueOf("foobar"),
                /* isKey= */ true)
            .get();

    KafkaJsonSchemaDeserializer<String> deserializer = new KafkaJsonSchemaDeserializer<>();
    deserializer.configure(SCHEMA_SERIALIZER_CONFIGS, /* isKey= */ true);
    Object deserialized = deserializer.deserialize(TOPIC_NAME, serialized.toByteArray());
    assertEquals("foobar", deserialized);
  }

  @Test
  public void serializeIntJsonschemaKey_returnsSerialized() throws Exception {
    JsonSchema schema = new JsonSchema("{\"type\": \"integer\"}");
    String subject = SUBJECT_NAME_STRATEGY.subjectName(TOPIC_NAME, /* isKey= */ true, schema);
    int schemaId = schemaRegistryClient.register(subject, schema);

    ByteString serialized =
        recordSerializer
            .serialize(
                EmbeddedFormat.JSONSCHEMA,
                TOPIC_NAME,
                Optional.of(RegisteredSchema.create(subject, schemaId, SCHEMA_VERSION, schema)),
                IntNode.valueOf(123),
                /* isKey= */ true)
            .get();

    KafkaJsonSchemaDeserializer<Integer> deserializer = new KafkaJsonSchemaDeserializer<>();
    deserializer.configure(SCHEMA_SERIALIZER_CONFIGS, /* isKey= */ true);
    Object deserialized = deserializer.deserialize(TOPIC_NAME, serialized.toByteArray());
    assertEquals(123, deserialized);
  }

  @Test
  public void serializeFloatJsonschemaKey_returnsSerialized() throws Exception {
    JsonSchema schema = new JsonSchema("{\"type\": \"number\"}");
    String subject = SUBJECT_NAME_STRATEGY.subjectName(TOPIC_NAME, /* isKey= */ true, schema);
    int schemaId = schemaRegistryClient.register(subject, schema);

    ByteString serialized =
        recordSerializer
            .serialize(
                EmbeddedFormat.JSONSCHEMA,
                TOPIC_NAME,
                Optional.of(RegisteredSchema.create(subject, schemaId, SCHEMA_VERSION, schema)),
                FloatNode.valueOf(123.456F),
                /* isKey= */ true)
            .get();

    KafkaJsonSchemaDeserializer<BigDecimal> deserializer = new KafkaJsonSchemaDeserializer<>();
    deserializer.configure(SCHEMA_SERIALIZER_CONFIGS, /* isKey= */ true);
    Object deserialized = deserializer.deserialize(TOPIC_NAME, serialized.toByteArray());
    assertEquals(new BigDecimal("123.456"), deserialized);
  }

  @Test
  public void serializeBooleanJsonschemaKey_returnsSerialized() throws Exception {
    JsonSchema schema = new JsonSchema("{\"type\": \"boolean\"}");
    String subject = SUBJECT_NAME_STRATEGY.subjectName(TOPIC_NAME, /* isKey= */ true, schema);
    int schemaId = schemaRegistryClient.register(subject, schema);

    ByteString serialized =
        recordSerializer
            .serialize(
                EmbeddedFormat.JSONSCHEMA,
                TOPIC_NAME,
                Optional.of(RegisteredSchema.create(subject, schemaId, SCHEMA_VERSION, schema)),
                BooleanNode.valueOf(true),
                /* isKey= */ true)
            .get();

    KafkaJsonSchemaDeserializer<Boolean> deserializer = new KafkaJsonSchemaDeserializer<>();
    deserializer.configure(SCHEMA_SERIALIZER_CONFIGS, /* isKey= */ true);
    Object deserialized = deserializer.deserialize(TOPIC_NAME, serialized.toByteArray());
    assertEquals(true, deserialized);
  }

  @Test
  public void serializeObjectJsonschemaKey_returnsSerialized() throws Exception {
    JsonSchema schema =
        new JsonSchema(
            "{"
                + "    \"type\": \"object\","
                + "    \"properties\": {"
                + "        \"foo\": {\"type\": \"integer\"},"
                + "        \"bar\": {\"type\": \"boolean\"}"
                + "    }"
                + "}");
    String subject = SUBJECT_NAME_STRATEGY.subjectName(TOPIC_NAME, /* isKey= */ true, schema);
    int schemaId = schemaRegistryClient.register(subject, schema);
    ObjectNode node = new ObjectNode(JsonNodeFactory.instance);
    node.put("foo", 1);
    node.put("bar", true);

    ByteString serialized =
        recordSerializer
            .serialize(
                EmbeddedFormat.JSONSCHEMA,
                TOPIC_NAME,
                Optional.of(RegisteredSchema.create(subject, schemaId, SCHEMA_VERSION, schema)),
                node,
                /* isKey= */ true)
            .get();

    KafkaJsonSchemaDeserializer<Map<String, Object>> deserializer =
        new KafkaJsonSchemaDeserializer<>();
    deserializer.configure(SCHEMA_SERIALIZER_CONFIGS, /* isKey= */ true);
    Map<String, Object> deserialized =
        deserializer.deserialize(TOPIC_NAME, serialized.toByteArray());
    HashMap<String, Object> expected = new HashMap<>();
    expected.put("foo", 1);
    expected.put("bar", true);
    assertEquals(expected, deserialized);
  }

  @Test
  public void serializeArrayJsonschemaKey_returnsSerialized() throws Exception {
    JsonSchema schema =
        new JsonSchema(
            "{" + "    \"type\": \"array\"," + "    \"items\": {\"type\": \"integer\"}" + "}");
    String subject = SUBJECT_NAME_STRATEGY.subjectName(TOPIC_NAME, /* isKey= */ true, schema);
    int schemaId = schemaRegistryClient.register(subject, schema);
    ArrayNode node = new ArrayNode(JsonNodeFactory.instance);
    node.add(1);
    node.add(2);

    ByteString serialized =
        recordSerializer
            .serialize(
                EmbeddedFormat.JSONSCHEMA,
                TOPIC_NAME,
                Optional.of(RegisteredSchema.create(subject, schemaId, SCHEMA_VERSION, schema)),
                node,
                /* isKey= */ true)
            .get();

    KafkaJsonSchemaDeserializer<List<Integer>> deserializer = new KafkaJsonSchemaDeserializer<>();
    deserializer.configure(SCHEMA_SERIALIZER_CONFIGS, /* isKey= */ true);
    List<Integer> deserialized = deserializer.deserialize(TOPIC_NAME, serialized.toByteArray());
    assertEquals(Arrays.asList(1, 2), deserialized);
  }

  @Test
  public void serializeNullJsonschemaKeyNullSchema_returnsEmpty() {
    Optional<ByteString> serialized =
        recordSerializer.serialize(
            EmbeddedFormat.JSONSCHEMA,
            TOPIC_NAME,
            /* schema= */ Optional.empty(),
            NullNode.getInstance(),
            /* isKey= */ true);

    assertFalse(serialized.isPresent());
  }

  @Test
  public void serializeNullJsonschemaKeyNonNullableSchema_returnsEmpty() throws Exception {
    JsonSchema schema = new JsonSchema("{\"type\": \"integer\"}");
    String subject = SUBJECT_NAME_STRATEGY.subjectName(TOPIC_NAME, /* isKey= */ true, schema);
    int schemaId = schemaRegistryClient.register(subject, schema);

    Optional<ByteString> serialized =
        recordSerializer.serialize(
            EmbeddedFormat.JSONSCHEMA,
            TOPIC_NAME,
            Optional.of(RegisteredSchema.create(subject, schemaId, SCHEMA_VERSION, schema)),
            NullNode.getInstance(),
            /* isKey= */ true);

    assertFalse(serialized.isPresent());
  }

  @Test
  public void serializeNonNullJsonschemaKeyNullSchema_throwsSerializationException() {
    assertThrows(
        RestConstraintViolationException.class,
        () ->
            recordSerializer.serialize(
                EmbeddedFormat.JSONSCHEMA,
                TOPIC_NAME,
                /* schema= */ Optional.empty(),
                TextNode.valueOf("foobar"),
                /* isKey= */ true));
  }

  @Test
  public void serializeInvalidJsonschemaKey_throwsBadRequestException() throws Exception {
    JsonSchema schema = new JsonSchema("{\"type\": \"integer\"}");
    String subject = SUBJECT_NAME_STRATEGY.subjectName(TOPIC_NAME, /* isKey= */ true, schema);
    int schemaId = schemaRegistryClient.register(subject, schema);

    assertThrows(
        BadRequestException.class,
        () ->
            recordSerializer.serialize(
                EmbeddedFormat.JSONSCHEMA,
                TOPIC_NAME,
                Optional.of(RegisteredSchema.create(subject, schemaId, SCHEMA_VERSION, schema)),
                TextNode.valueOf("foobar"),
                /* isKey= */ true));
  }

  @Test
  public void serializeStringJsonschemaValue_returnsSerialized() throws Exception {
    JsonSchema schema = new JsonSchema("{\"type\": \"string\"}");
    String subject = SUBJECT_NAME_STRATEGY.subjectName(TOPIC_NAME, /* isKey= */ false, schema);
    int schemaId = schemaRegistryClient.register(subject, schema);

    ByteString serialized =
        recordSerializer
            .serialize(
                EmbeddedFormat.JSONSCHEMA,
                TOPIC_NAME,
                Optional.of(RegisteredSchema.create(subject, schemaId, SCHEMA_VERSION, schema)),
                TextNode.valueOf("foobar"),
                /* isKey= */ false)
            .get();

    KafkaJsonSchemaDeserializer<String> deserializer = new KafkaJsonSchemaDeserializer<>();
    deserializer.configure(SCHEMA_SERIALIZER_CONFIGS, /* isKey= */ false);
    Object deserialized = deserializer.deserialize(TOPIC_NAME, serialized.toByteArray());
    assertEquals("foobar", deserialized);
  }

  @Test
  public void serializeIntJsonschemaValue_returnsSerialized() throws Exception {
    JsonSchema schema = new JsonSchema("{\"type\": \"integer\"}");
    String subject = SUBJECT_NAME_STRATEGY.subjectName(TOPIC_NAME, /* isKey= */ false, schema);
    int schemaId = schemaRegistryClient.register(subject, schema);

    ByteString serialized =
        recordSerializer
            .serialize(
                EmbeddedFormat.JSONSCHEMA,
                TOPIC_NAME,
                Optional.of(RegisteredSchema.create(subject, schemaId, SCHEMA_VERSION, schema)),
                IntNode.valueOf(123),
                /* isKey= */ false)
            .get();

    KafkaJsonSchemaDeserializer<Integer> deserializer = new KafkaJsonSchemaDeserializer<>();
    deserializer.configure(SCHEMA_SERIALIZER_CONFIGS, /* isKey= */ false);
    Object deserialized = deserializer.deserialize(TOPIC_NAME, serialized.toByteArray());
    assertEquals(123, deserialized);
  }

  @Test
  public void serializeFloatJsonschemaValue_returnsSerialized() throws Exception {
    JsonSchema schema = new JsonSchema("{\"type\": \"number\"}");
    String subject = SUBJECT_NAME_STRATEGY.subjectName(TOPIC_NAME, /* isKey= */ false, schema);
    int schemaId = schemaRegistryClient.register(subject, schema);

    ByteString serialized =
        recordSerializer
            .serialize(
                EmbeddedFormat.JSONSCHEMA,
                TOPIC_NAME,
                Optional.of(RegisteredSchema.create(subject, schemaId, SCHEMA_VERSION, schema)),
                FloatNode.valueOf(123.456F),
                /* isKey= */ false)
            .get();

    KafkaJsonSchemaDeserializer<BigDecimal> deserializer = new KafkaJsonSchemaDeserializer<>();
    deserializer.configure(SCHEMA_SERIALIZER_CONFIGS, /* isKey= */ false);
    Object deserialized = deserializer.deserialize(TOPIC_NAME, serialized.toByteArray());
    assertEquals(new BigDecimal("123.456"), deserialized);
  }

  @Test
  public void serializeBooleanJsonschemaValue_returnsSerialized() throws Exception {
    JsonSchema schema = new JsonSchema("{\"type\": \"boolean\"}");
    String subject = SUBJECT_NAME_STRATEGY.subjectName(TOPIC_NAME, /* isKey= */ false, schema);
    int schemaId = schemaRegistryClient.register(subject, schema);

    ByteString serialized =
        recordSerializer
            .serialize(
                EmbeddedFormat.JSONSCHEMA,
                TOPIC_NAME,
                Optional.of(RegisteredSchema.create(subject, schemaId, SCHEMA_VERSION, schema)),
                BooleanNode.valueOf(true),
                /* isKey= */ false)
            .get();

    KafkaJsonSchemaDeserializer<Boolean> deserializer = new KafkaJsonSchemaDeserializer<>();
    deserializer.configure(SCHEMA_SERIALIZER_CONFIGS, /* isKey= */ false);
    Object deserialized = deserializer.deserialize(TOPIC_NAME, serialized.toByteArray());
    assertEquals(true, deserialized);
  }

  @Test
  public void serializeObjectJsonschemaValue_returnsSerialized() throws Exception {
    JsonSchema schema =
        new JsonSchema(
            "{"
                + "    \"type\": \"object\","
                + "    \"properties\": {"
                + "        \"foo\": {\"type\": \"integer\"},"
                + "        \"bar\": {\"type\": \"boolean\"}"
                + "    }"
                + "}");
    String subject = SUBJECT_NAME_STRATEGY.subjectName(TOPIC_NAME, /* isKey= */ false, schema);
    int schemaId = schemaRegistryClient.register(subject, schema);
    ObjectNode node = new ObjectNode(JsonNodeFactory.instance);
    node.put("foo", 1);
    node.put("bar", true);

    ByteString serialized =
        recordSerializer
            .serialize(
                EmbeddedFormat.JSONSCHEMA,
                TOPIC_NAME,
                Optional.of(RegisteredSchema.create(subject, schemaId, SCHEMA_VERSION, schema)),
                node,
                /* isKey= */ false)
            .get();

    KafkaJsonSchemaDeserializer<Map<String, Object>> deserializer =
        new KafkaJsonSchemaDeserializer<>();
    deserializer.configure(SCHEMA_SERIALIZER_CONFIGS, /* isKey= */ false);
    Map<String, Object> deserialized =
        deserializer.deserialize(TOPIC_NAME, serialized.toByteArray());
    HashMap<String, Object> expected = new HashMap<>();
    expected.put("foo", 1);
    expected.put("bar", true);
    assertEquals(expected, deserialized);
  }

  @Test
  public void serializeArrayJsonschemaValue_returnsSerialized() throws Exception {
    JsonSchema schema =
        new JsonSchema(
            "{" + "    \"type\": \"array\"," + "    \"items\": {\"type\": \"integer\"}" + "}");
    String subject = SUBJECT_NAME_STRATEGY.subjectName(TOPIC_NAME, /* isKey= */ false, schema);
    int schemaId = schemaRegistryClient.register(subject, schema);
    ArrayNode node = new ArrayNode(JsonNodeFactory.instance);
    node.add(1);
    node.add(2);

    ByteString serialized =
        recordSerializer
            .serialize(
                EmbeddedFormat.JSONSCHEMA,
                TOPIC_NAME,
                Optional.of(RegisteredSchema.create(subject, schemaId, SCHEMA_VERSION, schema)),
                node,
                /* isKey= */ false)
            .get();

    KafkaJsonSchemaDeserializer<List<Integer>> deserializer = new KafkaJsonSchemaDeserializer<>();
    deserializer.configure(SCHEMA_SERIALIZER_CONFIGS, /* isKey= */ false);
    List<Integer> deserialized = deserializer.deserialize(TOPIC_NAME, serialized.toByteArray());
    assertEquals(Arrays.asList(1, 2), deserialized);
  }

  @Test
  public void serializeNullJsonschemaValueNullSchema_returnsEmpty() {
    Optional<ByteString> serialized =
        recordSerializer.serialize(
            EmbeddedFormat.JSONSCHEMA,
            TOPIC_NAME,
            /* schema= */ Optional.empty(),
            NullNode.getInstance(),
            /* isKey= */ false);

    assertFalse(serialized.isPresent());
  }

  @Test
  public void serializeNullJsonschemaValueNonNullableSchema_returnsEmpty() throws Exception {
    JsonSchema schema = new JsonSchema("{\"type\": \"integer\"}");
    String subject = SUBJECT_NAME_STRATEGY.subjectName(TOPIC_NAME, /* isKey= */ false, schema);
    int schemaId = schemaRegistryClient.register(subject, schema);

    Optional<ByteString> serialized =
        recordSerializer.serialize(
            EmbeddedFormat.JSONSCHEMA,
            TOPIC_NAME,
            Optional.of(RegisteredSchema.create(subject, schemaId, SCHEMA_VERSION, schema)),
            NullNode.getInstance(),
            /* isKey= */ false);

    assertFalse(serialized.isPresent());
  }

  @Test
  public void serializeNonNullJsonschemaValueNullSchema_throwsSerializationException() {
    assertThrows(
        RestConstraintViolationException.class,
        () ->
            recordSerializer.serialize(
                EmbeddedFormat.JSONSCHEMA,
                TOPIC_NAME,
                /* schema= */ Optional.empty(),
                TextNode.valueOf("foobar"),
                /* isKey= */ false));
  }

  @Test
  public void serializeInvalidJsonschemaValue_throwsBadRequestException() throws Exception {
    JsonSchema schema = new JsonSchema("{\"type\": \"integer\"}");
    String subject = SUBJECT_NAME_STRATEGY.subjectName(TOPIC_NAME, /* isKey= */ false, schema);
    int schemaId = schemaRegistryClient.register(subject, schema);

    assertThrows(
        BadRequestException.class,
        () ->
            recordSerializer.serialize(
                EmbeddedFormat.JSONSCHEMA,
                TOPIC_NAME,
                Optional.of(RegisteredSchema.create(subject, schemaId, SCHEMA_VERSION, schema)),
                TextNode.valueOf("foobar"),
                /* isKey= */ false));
  }

  @Test
  public void serializeObjectProtobufKey_returnsSerialized() throws Exception {
    ProtobufSchema schema =
        new ProtobufSchema(
            "syntax = \"proto3\"; message ObjectKey { int32 foo = 1; bool bar = 2; }");
    String subject = SUBJECT_NAME_STRATEGY.subjectName(TOPIC_NAME, /* isKey= */ true, schema);
    int schemaId = schemaRegistryClient.register(subject, schema);
    ObjectNode node = new ObjectNode(JsonNodeFactory.instance);
    node.put("foo", 1);
    node.put("bar", true);

    ByteString serialized =
        recordSerializer
            .serialize(
                EmbeddedFormat.PROTOBUF,
                TOPIC_NAME,
                Optional.of(RegisteredSchema.create(subject, schemaId, SCHEMA_VERSION, schema)),
                node,
                /* isKey= */ true)
            .get();

    KafkaProtobufDeserializer<Message> deserializer = new KafkaProtobufDeserializer<>();
    deserializer.configure(SCHEMA_SERIALIZER_CONFIGS, /* isKey= */ true);
    Message deserialized = deserializer.deserialize(TOPIC_NAME, serialized.toByteArray());
    DynamicMessage.Builder expected = DynamicMessage.newBuilder(schema.toDescriptor());
    expected.setField(schema.toDescriptor().findFieldByName("foo"), 1);
    expected.setField(schema.toDescriptor().findFieldByName("bar"), true);
    assertEquals(expected.build(), deserialized);
  }

  @Test
  public void serializeNullProtobfuKeyNullSchema_returnsEmpty() {
    Optional<ByteString> serialized =
        recordSerializer.serialize(
            EmbeddedFormat.PROTOBUF,
            TOPIC_NAME,
            /* schema= */ Optional.empty(),
            NullNode.getInstance(),
            /* isKey= */ true);

    assertFalse(serialized.isPresent());
  }

  @Test
  public void serializeNullProtobufKeyNonNullableSchema_returnsEmpty() throws Exception {
    ProtobufSchema schema =
        new ProtobufSchema("syntax = \"proto3\"; message NullKey { int32 foo = 1; bool bar = 2; }");
    String subject = SUBJECT_NAME_STRATEGY.subjectName(TOPIC_NAME, /* isKey= */ true, schema);
    int schemaId = schemaRegistryClient.register(subject, schema);

    Optional<ByteString> serialized =
        recordSerializer.serialize(
            EmbeddedFormat.PROTOBUF,
            TOPIC_NAME,
            Optional.of(RegisteredSchema.create(subject, schemaId, SCHEMA_VERSION, schema)),
            NullNode.getInstance(),
            /* isKey= */ true);

    assertFalse(serialized.isPresent());
  }

  @Test
  public void serializeNonNullProtobufKeyNullSchema_throwsSerializationException() {
    ObjectNode node = new ObjectNode(JsonNodeFactory.instance);
    node.put("foo", 1);
    node.put("bar", true);

    assertThrows(
        RestConstraintViolationException.class,
        () ->
            recordSerializer.serialize(
                EmbeddedFormat.PROTOBUF,
                TOPIC_NAME,
                /* schema= */ Optional.empty(),
                node,
                /* isKey= */ true));
  }

  @Test
  public void serializeInvalidProtobufKey_throwsBadRequestException() throws Exception {
    ProtobufSchema schema =
        new ProtobufSchema(
            "syntax = \"proto3\"; message InvalidKey { int32 foo = 1; bool bar = 2; }");
    String subject = SUBJECT_NAME_STRATEGY.subjectName(TOPIC_NAME, /* isKey= */ true, schema);
    int schemaId = schemaRegistryClient.register(subject, schema);
    ObjectNode node = new ObjectNode(JsonNodeFactory.instance);
    node.put("foo", "bar");

    assertThrows(
        BadRequestException.class,
        () ->
            recordSerializer.serialize(
                EmbeddedFormat.PROTOBUF,
                TOPIC_NAME,
                Optional.of(RegisteredSchema.create(subject, schemaId, SCHEMA_VERSION, schema)),
                node,
                /* isKey= */ true));
  }

  @Test
  public void serializeObjectProtobufValue_returnsSerialized() throws Exception {
    ProtobufSchema schema =
        new ProtobufSchema(
            "syntax = \"proto3\"; message ObjectValueKey { int32 foo = 1; bool bar = 2; }");
    String subject = SUBJECT_NAME_STRATEGY.subjectName(TOPIC_NAME, /* isKey= */ false, schema);
    int schemaId = schemaRegistryClient.register(subject, schema);
    ObjectNode node = new ObjectNode(JsonNodeFactory.instance);
    node.put("foo", 1);
    node.put("bar", true);

    ByteString serialized =
        recordSerializer
            .serialize(
                EmbeddedFormat.PROTOBUF,
                TOPIC_NAME,
                Optional.of(RegisteredSchema.create(subject, schemaId, SCHEMA_VERSION, schema)),
                node,
                /* isKey= */ false)
            .get();

    KafkaProtobufDeserializer<Message> deserializer = new KafkaProtobufDeserializer<>();
    deserializer.configure(SCHEMA_SERIALIZER_CONFIGS, /* isKey= */ false);
    Message deserialized = deserializer.deserialize(TOPIC_NAME, serialized.toByteArray());
    DynamicMessage.Builder expected = DynamicMessage.newBuilder(schema.toDescriptor());
    expected.setField(schema.toDescriptor().findFieldByName("foo"), 1);
    expected.setField(schema.toDescriptor().findFieldByName("bar"), true);
    assertEquals(expected.build(), deserialized);
  }

  @Test
  public void serializeNullProtobfuValueNullSchema_returnsEmpty() {
    Optional<ByteString> serialized =
        recordSerializer.serialize(
            EmbeddedFormat.PROTOBUF,
            TOPIC_NAME,
            /* schema= */ Optional.empty(),
            NullNode.getInstance(),
            /* isKey= */ false);

    assertFalse(serialized.isPresent());
  }

  @Test
  public void serializeNullProtobufValueNonNullableSchema_returnsEmpty() throws Exception {
    ProtobufSchema schema =
        new ProtobufSchema(
            "syntax = \"proto3\"; message NullValue { int32 foo = 1; bool bar = 2; }");
    String subject = SUBJECT_NAME_STRATEGY.subjectName(TOPIC_NAME, /* isKey= */ false, schema);
    int schemaId = schemaRegistryClient.register(subject, schema);

    Optional<ByteString> serialized =
        recordSerializer.serialize(
            EmbeddedFormat.PROTOBUF,
            TOPIC_NAME,
            Optional.of(RegisteredSchema.create(subject, schemaId, SCHEMA_VERSION, schema)),
            NullNode.getInstance(),
            /* isKey= */ false);

    assertFalse(serialized.isPresent());
  }

  @Test
  public void serializeNonNullProtobufValueNullSchema_throwsSerializationException() {
    ObjectNode node = new ObjectNode(JsonNodeFactory.instance);
    node.put("foo", 1);
    node.put("bar", true);

    assertThrows(
        RestConstraintViolationException.class,
        () ->
            recordSerializer.serialize(
                EmbeddedFormat.PROTOBUF,
                TOPIC_NAME,
                /* schema= */ Optional.empty(),
                node,
                /* isKey= */ false));
  }

  @Test
  public void serializeInvalidProtobufValue_throwsBadRequestException() throws Exception {
    ProtobufSchema schema =
        new ProtobufSchema(
            "syntax = \"proto3\"; message InvalidValue { int32 foo = 1; bool bar = 2; }");
    String subject = SUBJECT_NAME_STRATEGY.subjectName(TOPIC_NAME, /* isKey= */ false, schema);
    int schemaId = schemaRegistryClient.register(subject, schema);
    ObjectNode node = new ObjectNode(JsonNodeFactory.instance);
    node.put("foo", "bar");

    assertThrows(
        BadRequestException.class,
        () ->
            recordSerializer.serialize(
                EmbeddedFormat.PROTOBUF,
                TOPIC_NAME,
                Optional.of(RegisteredSchema.create(subject, schemaId, SCHEMA_VERSION, schema)),
                node,
                /* isKey= */ false));
  }
}
