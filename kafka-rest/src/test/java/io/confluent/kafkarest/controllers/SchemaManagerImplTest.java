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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.json.JsonSchemaProvider;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.confluent.kafka.serializers.subject.TopicNameStrategy;
import io.confluent.kafka.serializers.subject.strategy.SubjectNameStrategy;
import io.confluent.kafkarest.entities.EmbeddedFormat;
import io.confluent.kafkarest.entities.RegisteredSchema;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import io.confluent.rest.exceptions.RestConstraintViolationException;
import org.apache.kafka.common.errors.SerializationException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class SchemaManagerImplTest {
  private static final String TOPIC_NAME = "topic-1";
  private static final String KEY_SUBJECT = "topic-1-key";
  private static final String VALUE_SUBJECT = "topic-1-value";

  private MockSchemaRegistryClient schemaRegistryClient;
  private SchemaManager schemaManager;

  @Before
  public void setUp() {
    schemaRegistryClient =
        (MockSchemaRegistryClient)
            MockSchemaRegistry.getClientForScope(
                UUID.randomUUID().toString(),
                Arrays.asList(
                    new AvroSchemaProvider(),
                    new JsonSchemaProvider(),
                    new ProtobufSchemaProvider()));

    schemaManager = new SchemaManagerImpl(schemaRegistryClient, new TopicNameStrategy());
  }

  @After
  public void tearDown() {
    schemaRegistryClient.reset();
  }

  @Test
  public void getSchema_avro_schemaId() throws Exception {
    ParsedSchema schema = new AvroSchema("{\"type\": \"int\"}");
    int schemaId = schemaRegistryClient.register(KEY_SUBJECT, schema);
    int schemaVersion = schemaRegistryClient.getVersion(KEY_SUBJECT, schema);

    RegisteredSchema actual =
        schemaManager.getSchema(
            TOPIC_NAME,
            /* format= */ Optional.empty(),
            /* subject= */ Optional.empty(),
            /* subjectNameStrategy= */ Optional.empty(),
            /* schemaId= */ Optional.of(schemaId),
            /* schemaVersion= */ Optional.empty(),
            /* rawSchema= */ Optional.empty(),
            /* isKey= */ true);

    assertEquals(RegisteredSchema.create(KEY_SUBJECT, schemaId, schemaVersion, schema), actual);
  }

  @Test
  public void getSchema_avro_schemaId_subject() throws Exception {
    String subject = "my-subject";
    ParsedSchema schema = new AvroSchema("{\"type\": \"int\"}");
    int schemaId = schemaRegistryClient.register(subject, schema);
    int schemaVersion = schemaRegistryClient.getVersion(subject, schema);

    RegisteredSchema actual =
        schemaManager.getSchema(
            TOPIC_NAME,
            /* format= */ Optional.empty(),
            /* subject= */ Optional.of(subject),
            /* subjectNameStrategy= */ Optional.empty(),
            /* schemaId= */ Optional.of(schemaId),
            /* schemaVersion= */ Optional.empty(),
            /* rawSchema= */ Optional.empty(),
            /* isKey= */ true);

    assertEquals(RegisteredSchema.create(subject, schemaId, schemaVersion, schema), actual);
  }

  @Test
  public void getSchema_avro_schemaId_subjectNameStrategy() throws Exception {
    ParsedSchema schema = new AvroSchema("{\"type\": \"int\"}");
    SubjectNameStrategy strategy = new MySubjectNameStrategy();
    String subject = strategy.subjectName(TOPIC_NAME, /* isKey= */ true, /* schema= */ null);
    int schemaId = schemaRegistryClient.register(subject, schema);
    int schemaVersion = schemaRegistryClient.getVersion(subject, schema);

    RegisteredSchema actual =
        schemaManager.getSchema(
            TOPIC_NAME,
            /* format= */ Optional.empty(),
            /* subject= */ Optional.empty(),
            /* subjectNameStrategy= */ Optional.of(strategy),
            /* schemaId= */ Optional.of(schemaId),
            /* schemaVersion= */ Optional.empty(),
            /* rawSchema= */ Optional.empty(),
            /* isKey= */ true);

    assertEquals(RegisteredSchema.create(subject, schemaId, schemaVersion, schema), actual);
  }

  @Test
  public void getSchema_avro_schemaVersion() throws Exception {
    ParsedSchema schema = new AvroSchema("{\"type\": \"int\"}");
    int schemaId = schemaRegistryClient.register(KEY_SUBJECT, schema);
    int schemaVersion = schemaRegistryClient.getVersion(KEY_SUBJECT, schema);

    RegisteredSchema actual =
        schemaManager.getSchema(
            TOPIC_NAME,
            /* format= */ Optional.empty(),
            /* subject= */ Optional.empty(),
            /* subjectNameStrategy= */ Optional.empty(),
            /* schemaId= */ Optional.empty(),
            /* schemaVersion= */ Optional.of(schemaVersion),
            /* rawSchema= */ Optional.empty(),
            /* isKey= */ true);

    assertEquals(RegisteredSchema.create(KEY_SUBJECT, schemaId, schemaVersion, schema), actual);
  }

  @Test
  public void getSchema_avro_schemaVersion_subject() throws Exception {
    String subject = "my-subject";
    ParsedSchema schema = new AvroSchema("{\"type\": \"int\"}");
    int schemaId = schemaRegistryClient.register(subject, schema);
    int schemaVersion = schemaRegistryClient.getVersion(subject, schema);

    RegisteredSchema actual =
        schemaManager.getSchema(
            TOPIC_NAME,
            /* format= */ Optional.empty(),
            /* subject= */ Optional.of(subject),
            /* subjectNameStrategy= */ Optional.empty(),
            /* schemaId= */ Optional.empty(),
            /* schemaVersion= */ Optional.of(schemaVersion),
            /* rawSchema= */ Optional.empty(),
            /* isKey= */ true);

    assertEquals(RegisteredSchema.create(subject, schemaId, schemaVersion, schema), actual);
  }

  @Test
  public void getSchema_avro_schemaVersion_subjectNameStrategy() throws Exception {
    ParsedSchema schema = new AvroSchema("{\"type\": \"int\"}");
    SubjectNameStrategy strategy = new MySubjectNameStrategy();
    String subject = strategy.subjectName(TOPIC_NAME, /* isKey= */ true, /* schema= */ null);
    int schemaId = schemaRegistryClient.register(subject, schema);
    int schemaVersion = schemaRegistryClient.getVersion(subject, schema);

    RegisteredSchema actual =
        schemaManager.getSchema(
            TOPIC_NAME,
            /* format= */ Optional.empty(),
            /* subject= */ Optional.empty(),
            /* subjectNameStrategy= */ Optional.of(strategy),
            /* schemaId= */ Optional.empty(),
            /* schemaVersion= */ Optional.of(schemaVersion),
            /* rawSchema= */ Optional.empty(),
            /* isKey= */ true);

    assertEquals(RegisteredSchema.create(subject, schemaId, schemaVersion, schema), actual);
  }

  @Test
  public void getSchema_avro_rawSchema() throws Exception {
    RegisteredSchema actual =
        schemaManager.getSchema(
            TOPIC_NAME,
            /* format= */ Optional.of(EmbeddedFormat.AVRO),
            /* subject= */ Optional.empty(),
            /* subjectNameStrategy= */ Optional.empty(),
            /* schemaId= */ Optional.empty(),
            /* schemaVersion= */ Optional.empty(),
            /* rawSchema= */ Optional.of("{\"type\": \"int\"}"),
            /* isKey= */ true);

    ParsedSchema schema = schemaRegistryClient.getSchemaById(actual.getSchemaId());
    int schemaId = schemaRegistryClient.getId(KEY_SUBJECT, schema);
    int schemaVersion = schemaRegistryClient.getVersion(KEY_SUBJECT, schema);

    assertEquals(RegisteredSchema.create(KEY_SUBJECT, schemaId, schemaVersion, schema), actual);
  }

  @Test
  public void getSchema_avro_rawSchema_subject() throws Exception {
    String subject = "my-subject";
    RegisteredSchema actual =
        schemaManager.getSchema(
            TOPIC_NAME,
            /* format= */ Optional.of(EmbeddedFormat.AVRO),
            /* subject= */ Optional.of(subject),
            /* subjectNameStrategy= */ Optional.empty(),
            /* schemaId= */ Optional.empty(),
            /* schemaVersion= */ Optional.empty(),
            /* rawSchema= */ Optional.of("{\"type\": \"int\"}"),
            /* isKey= */ true);

    ParsedSchema schema = schemaRegistryClient.getSchemaById(actual.getSchemaId());
    int schemaId = schemaRegistryClient.getId(subject, schema);
    int schemaVersion = schemaRegistryClient.getVersion(subject, schema);

    assertEquals(RegisteredSchema.create(subject, schemaId, schemaVersion, schema), actual);
  }

  @Test
  public void getSchema_avro_rawSchema_subjectNameStrategy() throws Exception {
    SubjectNameStrategy strategy = new MySubjectNameStrategy();
    String subject = strategy.subjectName(TOPIC_NAME, /* isKey= */ true, /* schema= */ null);
    RegisteredSchema actual =
        schemaManager.getSchema(
            TOPIC_NAME,
            /* format= */ Optional.of(EmbeddedFormat.AVRO),
            /* subject= */ Optional.empty(),
            /* subjectNameStrategy= */ Optional.of(strategy),
            /* schemaId= */ Optional.empty(),
            /* schemaVersion= */ Optional.empty(),
            /* rawSchema= */ Optional.of("{\"type\": \"int\"}"),
            /* isKey= */ true);

    ParsedSchema schema = schemaRegistryClient.getSchemaById(actual.getSchemaId());
    int schemaId = schemaRegistryClient.getId(subject, schema);
    int schemaVersion = schemaRegistryClient.getVersion(subject, schema);

    assertEquals(RegisteredSchema.create(subject, schemaId, schemaVersion, schema), actual);
  }

  @Test
  public void getSchema_avro_latestSchema() throws Exception {
    ParsedSchema schema = new AvroSchema("{\"type\": \"int\"}");
    int schemaId = schemaRegistryClient.register(KEY_SUBJECT, schema);
    int schemaVersion = schemaRegistryClient.getVersion(KEY_SUBJECT, schema);

    RegisteredSchema actual =
        schemaManager.getSchema(
            TOPIC_NAME,
            /* format= */ Optional.empty(),
            /* subject= */ Optional.empty(),
            /* subjectNameStrategy= */ Optional.empty(),
            /* schemaId= */ Optional.empty(),
            /* schemaVersion= */ Optional.empty(),
            /* rawSchema= */ Optional.empty(),
            /* isKey= */ true);

    assertEquals(RegisteredSchema.create(KEY_SUBJECT, schemaId, schemaVersion, schema), actual);
  }

  @Test
  public void getSchema_avro_latestSchema_subject() throws Exception {
    String subject = "my-subject";
    ParsedSchema schema = new AvroSchema("{\"type\": \"int\"}");
    int schemaId = schemaRegistryClient.register(subject, schema);
    int schemaVersion = schemaRegistryClient.getVersion(subject, schema);

    RegisteredSchema actual =
        schemaManager.getSchema(
            TOPIC_NAME,
            /* format= */ Optional.empty(),
            /* subject= */ Optional.of(subject),
            /* subjectNameStrategy= */ Optional.empty(),
            /* schemaId= */ Optional.empty(),
            /* schemaVersion= */ Optional.empty(),
            /* rawSchema= */ Optional.empty(),
            /* isKey= */ true);

    assertEquals(RegisteredSchema.create(subject, schemaId, schemaVersion, schema), actual);
  }

  @Test
  public void getSchema_avro_latestSchema_subjectNameStrategy() throws Exception {
    ParsedSchema schema = new AvroSchema("{\"type\": \"int\"}");
    SubjectNameStrategy strategy = new MySubjectNameStrategy();
    String subject = strategy.subjectName(TOPIC_NAME, /* isKey= */ true, /* schema= */ null);
    int schemaId = schemaRegistryClient.register(subject, schema);
    int schemaVersion = schemaRegistryClient.getVersion(subject, schema);

    RegisteredSchema actual =
        schemaManager.getSchema(
            TOPIC_NAME,
            /* format= */ Optional.empty(),
            /* subject= */ Optional.empty(),
            /* subjectNameStrategy= */ Optional.of(strategy),
            /* schemaId= */ Optional.empty(),
            /* schemaVersion= */ Optional.empty(),
            /* rawSchema= */ Optional.empty(),
            /* isKey= */ true);

    assertEquals(RegisteredSchema.create(subject, schemaId, schemaVersion, schema), actual);
  }

  @Test
  public void getSchema_jsonschema_rawSchema() throws Exception {
    RegisteredSchema actual =
        schemaManager.getSchema(
            TOPIC_NAME,
            /* format= */ Optional.of(EmbeddedFormat.JSONSCHEMA),
            /* subject= */ Optional.empty(),
            /* subjectNameStrategy= */ Optional.empty(),
            /* schemaId= */ Optional.empty(),
            /* schemaVersion= */ Optional.empty(),
            /* rawSchema= */ Optional.of("{\"type\": \"string\"}"),
            /* isKey= */ true);

    ParsedSchema schema = schemaRegistryClient.getSchemaById(actual.getSchemaId());
    int schemaId = schemaRegistryClient.getId(KEY_SUBJECT, schema);
    int schemaVersion = schemaRegistryClient.getVersion(KEY_SUBJECT, schema);

    assertEquals(RegisteredSchema.create(KEY_SUBJECT, schemaId, schemaVersion, schema), actual);
  }

  @Test
  public void getSchema_protobuf_rawSchema() throws Exception {
    RegisteredSchema actual =
        schemaManager.getSchema(
            TOPIC_NAME,
            /* format= */ Optional.of(EmbeddedFormat.PROTOBUF),
            /* subject= */ Optional.empty(),
            /* subjectNameStrategy= */ Optional.empty(),
            /* schemaId= */ Optional.empty(),
            /* schemaVersion= */ Optional.empty(),
            /* rawSchema= */ Optional.of("syntax = \"proto3\"; message MyKey { string foo = 1; }"),
            /* isKey= */ true);

    ParsedSchema schema = schemaRegistryClient.getSchemaById(actual.getSchemaId());
    int schemaId = schemaRegistryClient.getId(KEY_SUBJECT, schema);
    int schemaVersion = schemaRegistryClient.getVersion(KEY_SUBJECT, schema);

    assertEquals(RegisteredSchema.create(KEY_SUBJECT, schemaId, schemaVersion, schema), actual);
  }

  @Test
  public void getSchema_avro_latestSchema_notIsKey() throws Exception {
    ParsedSchema schema = new AvroSchema("{\"type\": \"int\"}");
    int schemaId = schemaRegistryClient.register(VALUE_SUBJECT, schema);
    int schemaVersion = schemaRegistryClient.getVersion(VALUE_SUBJECT, schema);

    RegisteredSchema actual =
        schemaManager.getSchema(
            TOPIC_NAME,
            /* format= */ Optional.empty(),
            /* subject= */ Optional.empty(),
            /* subjectNameStrategy= */ Optional.empty(),
            /* schemaId= */ Optional.empty(),
            /* schemaVersion= */ Optional.empty(),
            /* rawSchema= */ Optional.empty(),
            /* isKey= */ false);

    assertEquals(RegisteredSchema.create(VALUE_SUBJECT, schemaId, schemaVersion, schema), actual);
  }

  @Test
  public void getSchema_avro_schemaId_nonExistingSchemaId() {
    assertThrows(
        SerializationException.class,
        () ->
            schemaManager.getSchema(
                TOPIC_NAME,
                /* format= */ Optional.empty(),
                /* subject= */ Optional.empty(),
                /* subjectNameStrategy= */ Optional.empty(),
                /* schemaId= */ Optional.of(1000),
                /* schemaVersion= */ Optional.empty(),
                /* rawSchema= */ Optional.empty(),
                /* isKey= */ true));
  }

  @Test
  public void getSchema_avro_schemaId_schemaIdNotInSubject() throws Exception {
    ParsedSchema schema = new AvroSchema("{\"type\": \"int\"}");
    int schemaId = schemaRegistryClient.register("foobar", schema);

    assertThrows(
        SerializationException.class,
        () ->
            schemaManager.getSchema(
                TOPIC_NAME,
                /* format= */ Optional.empty(),
                /* subject= */ Optional.empty(),
                /* subjectNameStrategy= */ Optional.empty(),
                /* schemaId= */ Optional.of(schemaId),
                /* schemaVersion= */ Optional.empty(),
                /* rawSchema= */ Optional.empty(),
                /* isKey= */ true));
  }

  @Test
  public void getSchema_avro_rawSchema_invalidSchema() {
    assertThrows(
        RestConstraintViolationException.class,
        () ->
            schemaManager.getSchema(
                TOPIC_NAME,
                /* format= */ Optional.of(EmbeddedFormat.AVRO),
                /* subject= */ Optional.empty(),
                /* subjectNameStrategy= */ Optional.empty(),
                /* schemaId= */ Optional.empty(),
                /* schemaVersion= */ Optional.empty(),
                /* rawSchema= */ Optional.of("foobar"),
                /* isKey= */ true));
  }

  @Test
  public void getSchema_jsonschema_rawSchema_invalidSchema() {
    assertThrows(
        RestConstraintViolationException.class,
        () ->
            schemaManager.getSchema(
                TOPIC_NAME,
                /* format= */ Optional.of(EmbeddedFormat.JSONSCHEMA),
                /* subject= */ Optional.empty(),
                /* subjectNameStrategy= */ Optional.empty(),
                /* schemaId= */ Optional.empty(),
                /* schemaVersion= */ Optional.empty(),
                /* rawSchema= */ Optional.of("foobar"),
                /* isKey= */ true));
  }

  @Test
  public void getSchema_protobuf_rawSchema_invalidSchema() {
    assertThrows(
        RestConstraintViolationException.class,
        () ->
            schemaManager.getSchema(
                TOPIC_NAME,
                /* format= */ Optional.of(EmbeddedFormat.PROTOBUF),
                /* subject= */ Optional.empty(),
                /* subjectNameStrategy= */ Optional.empty(),
                /* schemaId= */ Optional.empty(),
                /* schemaVersion= */ Optional.empty(),
                /* rawSchema= */ Optional.of("foobar"),
                /* isKey= */ true));
  }

  @Test
  public void getSchema_avro_latestSchema_noSchema() {
    assertThrows(
        SerializationException.class,
        () ->
            schemaManager.getSchema(
                TOPIC_NAME,
                /* format= */ Optional.empty(),
                /* subject= */ Optional.empty(),
                /* subjectNameStrategy= */ Optional.empty(),
                /* schemaId= */ Optional.empty(),
                /* schemaVersion= */ Optional.empty(),
                /* rawSchema= */ Optional.empty(),
                /* isKey= */ true));
  }

  @Test
  public void getSchema_avro_schemaVersion_subjectNameStrategy_strategyDependsOnSchema()
      throws Exception {
    ParsedSchema schema = new AvroSchema("{\"type\": \"int\"}");
    SubjectNameStrategy strategy = new SchemaDependentSubjectNameStrategy();
    String subject = strategy.subjectName(TOPIC_NAME, /* isKey= */ true, /* schema= */ schema);
    schemaRegistryClient.register(subject, schema);
    int schemaVersion = schemaRegistryClient.getVersion(subject, schema);

    assertThrows(
        IllegalArgumentException.class,
        () ->
            schemaManager.getSchema(
                TOPIC_NAME,
                /* format= */ Optional.empty(),
                /* subject= */ Optional.empty(),
                /* subjectNameStrategy= */ Optional.of(strategy),
                /* schemaId= */ Optional.empty(),
                /* schemaVersion= */ Optional.of(schemaVersion),
                /* rawSchema= */ Optional.empty(),
                /* isKey= */ true));
  }

  @Test
  public void getSchema_avro_schemaVersion_subjectNameStrategy_strategyReturnsNull() {
    SubjectNameStrategy strategy = new NullReturningSubjectNameStrategy();
    strategy.subjectName(TOPIC_NAME, /* isKey= */ true, /* schema= */ null);

    assertThrows(
        IllegalArgumentException.class,
        () ->
            schemaManager.getSchema(
                TOPIC_NAME,
                /* format= */ Optional.empty(),
                /* subject= */ Optional.empty(),
                /* subjectNameStrategy= */ Optional.of(strategy),
                /* schemaId= */ Optional.empty(),
                /* schemaVersion= */ Optional.of(100),
                /* rawSchema= */ Optional.empty(),
                /* isKey= */ true));
  }

  private static final class MySubjectNameStrategy implements SubjectNameStrategy {

    @Override
    public String subjectName(String topicName, boolean isKey, ParsedSchema schema) {
      return "my-subject-" + topicName + "-" + (isKey ? "key" : "value");
    }

    @Override
    public void configure(Map<String, ?> map) {
    }
  }

  private static final class SchemaDependentSubjectNameStrategy implements SubjectNameStrategy {

    @Override
    public String subjectName(String topicName, boolean isKey, ParsedSchema schema) {
      return "my-subject-" + schema.toString();
    }

    @Override
    public void configure(Map<String, ?> map) {
    }
  }

  private static final class NullReturningSubjectNameStrategy implements SubjectNameStrategy {

    @Override
    public String subjectName(String topicName, boolean isKey, ParsedSchema schema) {
      return null;
    }

    @Override
    public void configure(Map<String, ?> map) {
    }
  }
}
