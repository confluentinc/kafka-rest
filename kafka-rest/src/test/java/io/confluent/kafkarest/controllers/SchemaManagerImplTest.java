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

import static java.util.Collections.emptyList;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.replay;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.node.TextNode;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.SchemaProvider;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.json.JsonSchemaProvider;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.confluent.kafka.serializers.subject.TopicNameStrategy;
import io.confluent.kafka.serializers.subject.strategy.SubjectNameStrategy;
import io.confluent.kafkarest.entities.EmbeddedFormat;
import io.confluent.kafkarest.entities.RegisteredSchema;
import io.confluent.kafkarest.exceptions.BadRequestException;
import io.confluent.rest.exceptions.RestConstraintViolationException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.apache.avro.SchemaParseException;
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

    schemaManager =
        new SchemaManagerImpl(Optional.of(schemaRegistryClient), new TopicNameStrategy(), true);
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
        schemaManager
            .getSchema(
                TOPIC_NAME,
                /* format= */ Optional.empty(),
                /* subject= */ Optional.empty(),
                /* subjectNameStrategy= */ Optional.empty(),
                /* schemaId= */ Optional.of(schemaId),
                /* schemaVersion= */ Optional.empty(),
                /* rawSchema= */ Optional.empty(),
                /* isKey= */ true)
            .get();

    assertEquals(RegisteredSchema.create(KEY_SUBJECT, schemaId, schemaVersion, schema), actual);
  }

  @Test
  public void getSchema_avro_schemaId_subject() throws Exception {
    String subject = "my-subject";
    ParsedSchema schema = new AvroSchema("{\"type\": \"int\"}");
    int schemaId = schemaRegistryClient.register(subject, schema);
    int schemaVersion = schemaRegistryClient.getVersion(subject, schema);

    RegisteredSchema actual =
        schemaManager
            .getSchema(
                TOPIC_NAME,
                /* format= */ Optional.empty(),
                /* subject= */ Optional.of(subject),
                /* subjectNameStrategy= */ Optional.empty(),
                /* schemaId= */ Optional.of(schemaId),
                /* schemaVersion= */ Optional.empty(),
                /* rawSchema= */ Optional.empty(),
                /* isKey= */ true)
            .get();

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
        schemaManager
            .getSchema(
                TOPIC_NAME,
                /* format= */ Optional.empty(),
                /* subject= */ Optional.empty(),
                /* subjectNameStrategy= */ Optional.of(strategy),
                /* schemaId= */ Optional.of(schemaId),
                /* schemaVersion= */ Optional.empty(),
                /* rawSchema= */ Optional.empty(),
                /* isKey= */ true)
            .get();

    assertEquals(RegisteredSchema.create(subject, schemaId, schemaVersion, schema), actual);
  }

  @Test
  public void getSchema_avro_schemaVersion() throws Exception {
    ParsedSchema schema = new AvroSchema("{\"type\": \"int\"}");
    int schemaId = schemaRegistryClient.register(KEY_SUBJECT, schema);
    int schemaVersion = schemaRegistryClient.getVersion(KEY_SUBJECT, schema);

    RegisteredSchema actual =
        schemaManager
            .getSchema(
                TOPIC_NAME,
                /* format= */ Optional.empty(),
                /* subject= */ Optional.empty(),
                /* subjectNameStrategy= */ Optional.empty(),
                /* schemaId= */ Optional.empty(),
                /* schemaVersion= */ Optional.of(schemaVersion),
                /* rawSchema= */ Optional.empty(),
                /* isKey= */ true)
            .get();

    assertEquals(RegisteredSchema.create(KEY_SUBJECT, schemaId, schemaVersion, schema), actual);
  }

  @Test
  public void getSchema_avro_schemaVersion_subject() throws Exception {
    String subject = "my-subject";
    ParsedSchema schema = new AvroSchema("{\"type\": \"int\"}");
    int schemaId = schemaRegistryClient.register(subject, schema);
    int schemaVersion = schemaRegistryClient.getVersion(subject, schema);

    RegisteredSchema actual =
        schemaManager
            .getSchema(
                TOPIC_NAME,
                /* format= */ Optional.empty(),
                /* subject= */ Optional.of(subject),
                /* subjectNameStrategy= */ Optional.empty(),
                /* schemaId= */ Optional.empty(),
                /* schemaVersion= */ Optional.of(schemaVersion),
                /* rawSchema= */ Optional.empty(),
                /* isKey= */ true)
            .get();

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
        schemaManager
            .getSchema(
                TOPIC_NAME,
                /* format= */ Optional.empty(),
                /* subject= */ Optional.empty(),
                /* subjectNameStrategy= */ Optional.of(strategy),
                /* schemaId= */ Optional.empty(),
                /* schemaVersion= */ Optional.of(schemaVersion),
                /* rawSchema= */ Optional.empty(),
                /* isKey= */ true)
            .get();

    assertEquals(RegisteredSchema.create(subject, schemaId, schemaVersion, schema), actual);
  }

  @Test
  public void getSchema_avro_rawSchema() throws Exception {
    RegisteredSchema actual =
        schemaManager
            .getSchema(
                TOPIC_NAME,
                /* format= */ Optional.of(EmbeddedFormat.AVRO),
                /* subject= */ Optional.empty(),
                /* subjectNameStrategy= */ Optional.empty(),
                /* schemaId= */ Optional.empty(),
                /* schemaVersion= */ Optional.empty(),
                /* rawSchema= */ Optional.of("{\"type\": \"int\"}"),
                /* isKey= */ true)
            .get();

    ParsedSchema schema = schemaRegistryClient.getSchemaById(actual.getSchemaId());
    int schemaId = schemaRegistryClient.getId(KEY_SUBJECT, schema);
    int schemaVersion = schemaRegistryClient.getVersion(KEY_SUBJECT, schema);

    assertEquals(RegisteredSchema.create(KEY_SUBJECT, schemaId, schemaVersion, schema), actual);
  }

  @Test
  public void getSchema_avro_rawSchema_subject() throws Exception {
    String subject = "my-subject";
    RegisteredSchema actual =
        schemaManager
            .getSchema(
                TOPIC_NAME,
                /* format= */ Optional.of(EmbeddedFormat.AVRO),
                /* subject= */ Optional.of(subject),
                /* subjectNameStrategy= */ Optional.empty(),
                /* schemaId= */ Optional.empty(),
                /* schemaVersion= */ Optional.empty(),
                /* rawSchema= */ Optional.of("{\"type\": \"int\"}"),
                /* isKey= */ true)
            .get();

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
        schemaManager
            .getSchema(
                TOPIC_NAME,
                /* format= */ Optional.of(EmbeddedFormat.AVRO),
                /* subject= */ Optional.empty(),
                /* subjectNameStrategy= */ Optional.of(strategy),
                /* schemaId= */ Optional.empty(),
                /* schemaVersion= */ Optional.empty(),
                /* rawSchema= */ Optional.of("{\"type\": \"int\"}"),
                /* isKey= */ true)
            .get();

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
        schemaManager
            .getSchema(
                TOPIC_NAME,
                /* format= */ Optional.empty(),
                /* subject= */ Optional.empty(),
                /* subjectNameStrategy= */ Optional.empty(),
                /* schemaId= */ Optional.empty(),
                /* schemaVersion= */ Optional.empty(),
                /* rawSchema= */ Optional.empty(),
                /* isKey= */ true)
            .get();

    assertEquals(RegisteredSchema.create(KEY_SUBJECT, schemaId, schemaVersion, schema), actual);
  }

  @Test
  public void getSchema_avro_latestSchema_subject() throws Exception {
    String subject = "my-subject";
    ParsedSchema schema = new AvroSchema("{\"type\": \"int\"}");
    int schemaId = schemaRegistryClient.register(subject, schema);
    int schemaVersion = schemaRegistryClient.getVersion(subject, schema);

    RegisteredSchema actual =
        schemaManager
            .getSchema(
                TOPIC_NAME,
                /* format= */ Optional.empty(),
                /* subject= */ Optional.of(subject),
                /* subjectNameStrategy= */ Optional.empty(),
                /* schemaId= */ Optional.empty(),
                /* schemaVersion= */ Optional.empty(),
                /* rawSchema= */ Optional.empty(),
                /* isKey= */ true)
            .get();

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
        schemaManager
            .getSchema(
                TOPIC_NAME,
                /* format= */ Optional.empty(),
                /* subject= */ Optional.empty(),
                /* subjectNameStrategy= */ Optional.of(strategy),
                /* schemaId= */ Optional.empty(),
                /* schemaVersion= */ Optional.empty(),
                /* rawSchema= */ Optional.empty(),
                /* isKey= */ true)
            .get();

    assertEquals(RegisteredSchema.create(subject, schemaId, schemaVersion, schema), actual);
  }

  @Test
  public void getSchema_jsonschema_rawSchema() throws Exception {
    RegisteredSchema actual =
        schemaManager
            .getSchema(
                TOPIC_NAME,
                /* format= */ Optional.of(EmbeddedFormat.JSONSCHEMA),
                /* subject= */ Optional.empty(),
                /* subjectNameStrategy= */ Optional.empty(),
                /* schemaId= */ Optional.empty(),
                /* schemaVersion= */ Optional.empty(),
                /* rawSchema= */ Optional.of("{\"type\": \"string\"}"),
                /* isKey= */ true)
            .get();

    ParsedSchema schema = schemaRegistryClient.getSchemaById(actual.getSchemaId());
    int schemaId = schemaRegistryClient.getId(KEY_SUBJECT, schema);
    int schemaVersion = schemaRegistryClient.getVersion(KEY_SUBJECT, schema);

    assertEquals(RegisteredSchema.create(KEY_SUBJECT, schemaId, schemaVersion, schema), actual);
  }

  @Test
  public void getSchema_protobuf_rawSchema() throws Exception {
    RegisteredSchema actual =
        schemaManager
            .getSchema(
                TOPIC_NAME,
                /* format= */ Optional.of(EmbeddedFormat.PROTOBUF),
                /* subject= */ Optional.empty(),
                /* subjectNameStrategy= */ Optional.empty(),
                /* schemaId= */ Optional.empty(),
                /* schemaVersion= */ Optional.empty(),
                /* rawSchema= */ Optional.of(
                    "syntax = \"proto3\"; message MyKey { string foo = 1; }"),
                /* isKey= */ true)
            .get();

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
        schemaManager
            .getSchema(
                TOPIC_NAME,
                /* format= */ Optional.empty(),
                /* subject= */ Optional.empty(),
                /* subjectNameStrategy= */ Optional.empty(),
                /* schemaId= */ Optional.empty(),
                /* schemaVersion= */ Optional.empty(),
                /* rawSchema= */ Optional.empty(),
                /* isKey= */ false)
            .get();

    assertEquals(RegisteredSchema.create(VALUE_SUBJECT, schemaId, schemaVersion, schema), actual);
  }

  @Test
  public void getSchema_avro_schemaId_nonExistingSchemaId() {
    boolean checkpoint = false;
    try {
      schemaManager.getSchema(
          TOPIC_NAME,
          /* format= */ Optional.empty(),
          /* subject= */ Optional.empty(),
          /* subjectNameStrategy= */ Optional.empty(),
          /* schemaId= */ Optional.of(1000),
          /* schemaVersion= */ Optional.empty(),
          /* rawSchema= */ Optional.empty(),
          /* isKey= */ true);
    } catch (RestConstraintViolationException rcve) {
      assertEquals(
          "Error deserializing message. Error when fetching schema by id. schemaId = 1000",
          rcve.getMessage());
      assertEquals(42207, rcve.getErrorCode());
      checkpoint = true;
    }
    assertTrue(checkpoint);
  }

  @Test
  public void getSchema_avro_schemaId_schemaIdNotInSubject() throws Exception {
    ParsedSchema schema = new AvroSchema("{\"type\": \"int\"}");
    int schemaId = schemaRegistryClient.register("foobar", schema);

    boolean checkpoint = false;
    try {
      schemaManager.getSchema(
          TOPIC_NAME,
          /* format= */ Optional.empty(),
          /* subject= */ Optional.empty(),
          /* subjectNameStrategy= */ Optional.empty(),
          /* schemaId= */ Optional.of(schemaId),
          /* schemaVersion= */ Optional.empty(),
          /* rawSchema= */ Optional.empty(),
          /* isKey= */ true);
    } catch (RestConstraintViolationException rcve) {
      assertEquals(
          "Error deserializing message. Error when fetching schema version. subject = topic-1-key, schema = \n"
              + "Subject Not Found; error code: 40401",
          rcve.getMessage());
      assertEquals(42207, rcve.getErrorCode());
      checkpoint = true;
    }
    assertTrue(checkpoint);
  }

  @Test
  public void getSchema_avro_rawSchema_invalidSchema() {
    boolean checkpoint = false;
    try {
      schemaManager.getSchema(
          TOPIC_NAME,
          /* format= */ Optional.of(EmbeddedFormat.AVRO),
          /* subject= */ Optional.empty(),
          /* subjectNameStrategy= */ Optional.empty(),
          /* schemaId= */ Optional.empty(),
          /* schemaVersion= */ Optional.empty(),
          /* rawSchema= */ Optional.of("foobar"),
          /* isKey= */ true);

    } catch (RestConstraintViolationException rcve) {
      assertEquals(
          "Invalid schema: Error when parsing raw schema. format = AVRO, schema = foobar",
          rcve.getMessage());
      assertEquals(42205, rcve.getErrorCode());
      checkpoint = true;
    }
    assertTrue(checkpoint);
  }

  @Test
  public void getSchema_jsonschema_rawSchema_invalidSchema() {
    boolean checkpoint = false;
    try {
      schemaManager.getSchema(
          TOPIC_NAME,
          /* format= */ Optional.of(EmbeddedFormat.JSONSCHEMA),
          /* subject= */ Optional.empty(),
          /* subjectNameStrategy= */ Optional.empty(),
          /* schemaId= */ Optional.empty(),
          /* schemaVersion= */ Optional.empty(),
          /* rawSchema= */ Optional.of("foobar"),
          /* isKey= */ true);
    } catch (RestConstraintViolationException rcve) {
      assertEquals(
          "Invalid schema: Error when parsing raw schema. format = JSONSCHEMA, schema = foobar",
          rcve.getMessage());
      assertEquals(42205, rcve.getErrorCode());
      checkpoint = true;
    }
    assertTrue(checkpoint);
  }

  @Test
  public void getSchema_protobuf_rawSchema_invalidSchema() {
    boolean checkpoint = false;
    try {
      schemaManager.getSchema(
          TOPIC_NAME,
          /* format= */ Optional.of(EmbeddedFormat.PROTOBUF),
          /* subject= */ Optional.empty(),
          /* subjectNameStrategy= */ Optional.empty(),
          /* schemaId= */ Optional.empty(),
          /* schemaVersion= */ Optional.empty(),
          /* rawSchema= */ Optional.of("foobar"),
          /* isKey= */ true);
    } catch (RestConstraintViolationException rcve) {
      assertEquals(
          "Invalid schema: Error when parsing raw schema. format = PROTOBUF, schema = foobar",
          rcve.getMessage());
      assertEquals(42205, rcve.getErrorCode());
      checkpoint = true;
    }
    assertTrue(checkpoint);
  }

  @Test
  public void getSchema_avro_latestSchema_noSchema() {
    boolean checkpoint = false;
    try {
      schemaManager.getSchema(
          TOPIC_NAME,
          /* format= */ Optional.empty(),
          /* subject= */ Optional.empty(),
          /* subjectNameStrategy= */ Optional.empty(),
          /* schemaId= */ Optional.empty(),
          /* schemaVersion= */ Optional.empty(),
          /* rawSchema= */ Optional.empty(),
          /* isKey= */ true);

    } catch (RestConstraintViolationException rcve) {
      assertEquals(
          "Error deserializing message. Error when fetching latest schema version. subject = topic-1-key\n"
              + "Subject Not Found; error code: 40401",
          rcve.getMessage());
      assertEquals(42207, rcve.getErrorCode());
      checkpoint = true;
    }
    assertTrue(checkpoint);
  }

  @Test
  public void getSchema_avro_schemaVersion_subjectNameStrategy_strategyDependsOnSchema()
      throws Exception {
    ParsedSchema schema = new AvroSchema("{\"type\": \"int\"}");
    SubjectNameStrategy strategy = new SchemaDependentSubjectNameStrategy();
    String subject = strategy.subjectName(TOPIC_NAME, /* isKey= */ true, /* schema= */ schema);
    schemaRegistryClient.register(subject, schema);
    int schemaVersion = schemaRegistryClient.getVersion(subject, schema);

    boolean checkpoint = false;
    try {
      schemaManager.getSchema(
          TOPIC_NAME,
          /* format= */ Optional.empty(),
          /* subject= */ Optional.empty(),
          /* subjectNameStrategy= */ Optional.of(strategy),
          /* schemaId= */ Optional.empty(),
          /* schemaVersion= */ Optional.of(schemaVersion),
          /* rawSchema= */ Optional.empty(),
          /* isKey= */ true);
    } catch (BadRequestException bre) {
      assertEquals("Schema does not exist for subject: my-subject-, version: 1", bre.getMessage());
      assertEquals(400, bre.getCode());
      checkpoint = true;
    }
    assertTrue(checkpoint);
  }

  @Test
  public void getSchema_avro_schemaVersion_subjectNameStrategy_strategyReturnsNull() {
    SubjectNameStrategy strategy = new NullReturningSubjectNameStrategy();
    strategy.subjectName(TOPIC_NAME, /* isKey= */ true, /* schema= */ null);

    boolean checkpoint = false;
    try {
      schemaManager.getSchema(
          TOPIC_NAME,
          /* format= */ Optional.empty(),
          /* subject= */ Optional.empty(),
          /* subjectNameStrategy= */ Optional.of(strategy),
          /* schemaId= */ Optional.empty(),
          /* schemaVersion= */ Optional.of(100),
          /* rawSchema= */ Optional.empty(),
          /* isKey= */ true);
    } catch (IllegalArgumentException rcve) {
      assertTrue(
          rcve.getMessage()
              .startsWith(
                  "Cannot use schema_subject_strategy=io.confluent.kafkarest.controllers.SchemaManagerImplTest$NullReturningSubjectNameStrategy@"));
      assertTrue(rcve.getMessage().endsWith(" without schema_id or schema."));
      checkpoint = true;
    }
    assertTrue(checkpoint);
  }

  @Test
  public void schemaRegistryDisabledReturnsError() {
    SchemaManager mySchemaManager =
        new SchemaManagerImpl(Optional.empty(), new TopicNameStrategy(), false);
    boolean checkpoint = false;

    try {
      mySchemaManager.getSchema(
          TOPIC_NAME,
          /* format= */ Optional.empty(),
          /* subject= */ Optional.empty(),
          /* subjectNameStrategy= */ Optional.empty(),
          /* schemaId= */ Optional.empty(),
          /* schemaVersion= */ Optional.empty(),
          /* rawSchema= */ Optional.empty(),
          /* isKey= */ true);
    } catch (RestConstraintViolationException e) {
      assertEquals(
          "Payload error. Schema registry must be configured when using schemas.", e.getMessage());
      assertEquals(42206, e.getErrorCode());
      checkpoint = true;
    }
    assertTrue(checkpoint);
  }

  @Test
  public void rawSchemaWithUnsupportedSchemaVersionThrowsException() {
    boolean checkpoint = false;
    try {
      schemaManager.getSchema(
          TOPIC_NAME,
          /* format= */ Optional.empty(),
          /* subject= */ Optional.empty(),
          /* subjectNameStrategy= */ Optional.empty(),
          /* schemaId= */ Optional.empty(),
          /* schemaVersion= */ Optional.of(0),
          /* rawSchema= */ Optional.empty(),
          /* isKey= */ true);
    } catch (BadRequestException iae) {
      assertEquals("Schema does not exist for subject: topic-1-key, version: 0", iae.getMessage());
      assertEquals(400, iae.getCode());
      checkpoint = true;
    }
    assertTrue(checkpoint);
  }

  @Test
  public void getSchemaFromSchemaVersionThrowsInvalidSchemaException() {

    SchemaRegistryClient schemaRegistryClientMock = mock(SchemaRegistryClient.class);
    Schema schemaMock = mock(Schema.class);

    expect(schemaRegistryClientMock.getByVersion("subject1", 0, false)).andReturn(schemaMock);
    expect(schemaMock.getSchemaType()).andReturn(EmbeddedFormat.AVRO.toString());
    expect(schemaMock.getSchema()).andReturn(null);
    expect(schemaMock.getReferences()).andReturn(Collections.emptyList());

    replay(schemaRegistryClientMock, schemaMock);

    SchemaManager mySchemaManager =
        new SchemaManagerImpl(Optional.of(schemaRegistryClientMock), new TopicNameStrategy(), true);

    boolean checkpoint = false;
    try {
      mySchemaManager.getSchema(
          TOPIC_NAME,
          /* format= */ Optional.empty(),
          /* subject= */ Optional.of("subject1"),
          /* subjectNameStrategy= */ Optional.empty(),
          /* schemaId= */ Optional.empty(),
          /* schemaVersion= */ Optional.of(0),
          /* rawSchema= */ Optional.empty(),
          /* isKey= */ true);
    } catch (RestConstraintViolationException iae) {
      assertEquals(
          "Invalid schema: Error when fetching schema by version. subject = subject1, version = 0",
          iae.getMessage());
      assertEquals(42205, iae.getErrorCode());
      checkpoint = true;
    }
    assertTrue(checkpoint);
  }

  @Test
  public void getSchemaFromSchemaVersionThrowsInvalidBadRequestException() {

    SchemaRegistryClient schemaRegistryClientMock = mock(SchemaRegistryClient.class);
    Schema schemaMock = mock(Schema.class);

    expect(schemaRegistryClientMock.getByVersion("subject1", 0, false)).andReturn(schemaMock);
    expect(schemaMock.getSchemaType())
        .andThrow(new UnsupportedOperationException("exception message"));
    expect(schemaMock.getSchemaType()).andReturn("JSON");

    replay(schemaRegistryClientMock, schemaMock);

    SchemaManager mySchemaManager =
        new SchemaManagerImpl(Optional.of(schemaRegistryClientMock), new TopicNameStrategy(), true);

    boolean checkpoint = false;
    try {
      mySchemaManager.getSchema(
          TOPIC_NAME,
          /* format= */ Optional.empty(),
          /* subject= */ Optional.of("subject1"),
          /* subjectNameStrategy= */ Optional.empty(),
          /* schemaId= */ Optional.empty(),
          /* schemaVersion= */ Optional.of(0),
          /* rawSchema= */ Optional.empty(),
          /* isKey= */ true);
    } catch (BadRequestException iae) {
      assertEquals("Schema version not supported for JSON", iae.getMessage());
      assertEquals(400, iae.getCode());
      checkpoint = true;
    }
    assertTrue(checkpoint);
  }

  @Test
  public void errorFetchingSchemaBySchemaVersion() {

    SchemaRegistryClient schemaRegistryClientMock = mock(SchemaRegistryClient.class);
    Schema schemaMock = mock(Schema.class);

    expect(schemaRegistryClientMock.getByVersion("subject1", 123, false)).andReturn(schemaMock);
    expect(schemaMock.getSchemaType()).andReturn(EmbeddedFormat.JSON.toString());
    expect(schemaMock.getSchema()).andReturn(null);
    expect(schemaMock.getReferences()).andReturn(Collections.emptyList());
    replay(schemaRegistryClientMock, schemaMock);

    SchemaManager mySchemaManager =
        new SchemaManagerImpl(Optional.of(schemaRegistryClientMock), new TopicNameStrategy(), true);

    boolean checkpoint = false;
    try {
      mySchemaManager.getSchema(
          TOPIC_NAME,
          /* format= */ Optional.empty(),
          /* subject= */ Optional.of("subject1"),
          /* subjectNameStrategy= */ Optional.empty(),
          /* schemaId= */ Optional.empty(),
          /* schemaVersion= */ Optional.of(123),
          /* rawSchema= */ Optional.empty(),
          /* isKey= */ true);
    } catch (RestConstraintViolationException iae) {
      assertEquals(
          "Invalid schema: Error when fetching schema by version. subject = subject1, version = 123",
          iae.getMessage());
      assertEquals(42205, iae.getErrorCode());
      checkpoint = true;
    }
    assertTrue(checkpoint);
  }

  @Test
  public void errorRawSchemaNotSupportedWithFormat() {

    boolean checkpoint = false;
    try {
      schemaManager.getSchema(
          TOPIC_NAME,
          /* format= */ Optional.of(EmbeddedFormat.JSON),
          /* subject= */ Optional.empty(),
          /* subjectNameStrategy= */ Optional.empty(),
          /* schemaId= */ Optional.empty(),
          /* schemaVersion= */ Optional.empty(),
          /* rawSchema= */ Optional.of("rawSchema"),
          /* isKey= */ true);
    } catch (IllegalArgumentException iae) {
      assertEquals("JSON does not support schemas.", iae.getMessage());
      checkpoint = true;
    }
    assertTrue(checkpoint);
  }

  @Test
  public void errorRawSchemaCantParseSchema() {

    SchemaRegistryClient schemaRegistryClientMock = mock(SchemaRegistryClient.class);
    EmbeddedFormat embeddedFormatMock = mock(EmbeddedFormat.class);
    SchemaProvider schemaProviderMock = mock(SchemaProvider.class);

    expect(embeddedFormatMock.requiresSchema()).andReturn(true);
    expect(embeddedFormatMock.getSchemaProvider())
        .andThrow(new SchemaParseException("parse gone wrong"));

    replay(embeddedFormatMock, schemaProviderMock, schemaRegistryClientMock);

    SchemaManager mySchemaManager =
        new SchemaManagerImpl(Optional.of(schemaRegistryClientMock), new TopicNameStrategy(), true);

    boolean checkpoint = false;
    try {
      mySchemaManager.getSchema(
          TOPIC_NAME,
          /* format= */ Optional.of(embeddedFormatMock),
          /* subject= */ Optional.empty(),
          /* subjectNameStrategy= */ Optional.empty(),
          /* schemaId= */ Optional.empty(),
          /* schemaVersion= */ Optional.empty(),
          /* rawSchema= */ Optional.of(TextNode.valueOf("rawSchema").toString()),
          /* isKey= */ true);
    } catch (BadRequestException rcve) {
      assertEquals(
          "Raw schema not supported with EasyMock for class io.confluent.kafkarest.entities.EmbeddedFormat",
          rcve.getMessage());
      assertEquals(400, rcve.getCode());
      checkpoint = true;
    }
    assertTrue(checkpoint);
  }

  @Test
  public void errorRawSchemaNotSupportedWithSchema() {

    EmbeddedFormat embeddedFormatMock = mock(EmbeddedFormat.class);
    SchemaProvider schemaProviderMock = mock(SchemaProvider.class);

    expect(embeddedFormatMock.requiresSchema()).andReturn(true);
    expect(embeddedFormatMock.getSchemaProvider())
        .andThrow(new UnsupportedOperationException("Reason here"));
    expect(
            schemaProviderMock.parseSchema(
                TextNode.valueOf("rawSchema").toString(), emptyList(), true))
        .andReturn(Optional.empty());

    replay(embeddedFormatMock, schemaProviderMock);

    boolean checkpoint = false;
    try {
      schemaManager.getSchema(
          TOPIC_NAME,
          /* format= */ Optional.of(embeddedFormatMock),
          /* subject= */ Optional.empty(),
          /* subjectNameStrategy= */ Optional.empty(),
          /* schemaId= */ Optional.empty(),
          /* schemaVersion= */ Optional.empty(),
          /* rawSchema= */ Optional.of(TextNode.valueOf("rawSchema").toString()),
          /* isKey= */ true);
    } catch (BadRequestException bre) {
      assertEquals(
          "Raw schema not supported with EasyMock for class io.confluent.kafkarest.entities.EmbeddedFormat",
          bre.getMessage());
      assertEquals(400, bre.getCode());
      checkpoint = true;
    }
    assertTrue(checkpoint);
  }

  @Test
  public void errorRegisteringSchema() throws RestClientException, IOException {

    SchemaRegistryClient schemaRegistryClientMock = mock(SchemaRegistryClient.class);
    ParsedSchema parsedSchemaMock = mock(ParsedSchema.class);
    EmbeddedFormat embeddedFormatMock = mock(EmbeddedFormat.class);
    SchemaProvider schemaProviderMock = mock(SchemaProvider.class);

    expect(embeddedFormatMock.requiresSchema()).andReturn(true);
    expect(embeddedFormatMock.getSchemaProvider()).andReturn(schemaProviderMock);
    expect(
            schemaProviderMock.parseSchema(
                TextNode.valueOf("rawString").toString(), emptyList(), true))
        .andReturn(Optional.of(parsedSchemaMock));
    expect(schemaRegistryClientMock.getId("subject1", parsedSchemaMock))
        .andThrow(new IOException("Can't get Schema"));
    expect(schemaRegistryClientMock.register("subject1", parsedSchemaMock))
        .andThrow(new IOException("Can't register Schema"));

    replay(schemaRegistryClientMock, embeddedFormatMock, schemaProviderMock);

    SchemaManager mySchemaManager =
        new SchemaManagerImpl(Optional.of(schemaRegistryClientMock), new TopicNameStrategy(), true);

    boolean checkpoint = false;
    try {
      mySchemaManager.getSchema(
          TOPIC_NAME,
          /* format= */ Optional.of(embeddedFormatMock),
          /* subject= */ Optional.of("subject1"),
          /* subjectNameStrategy= */ Optional.empty(),
          /* schemaId= */ Optional.empty(),
          /* schemaVersion= */ Optional.empty(),
          /* rawSchema= */ Optional.of(TextNode.valueOf("rawString").toString()),
          /* isKey= */ true);
    } catch (RestConstraintViolationException rcve) {
      assertEquals(
          "Error deserializing message. Error when registering schema. format = EasyMock for class io.confluent.kafkarest.entities.EmbeddedFormat, subject = subject1, schema = null\n"
              + "Can't register Schema",
          rcve.getMessage());
      assertEquals(42207, rcve.getErrorCode());
      checkpoint = true;
    }
    assertTrue(checkpoint);
  }

  @Test
  public void errorFetchingLatestSchemaBySchemaVersionInvalidSchema()
      throws RestClientException, IOException {

    SchemaRegistryClient schemaRegistryClientMock = mock(SchemaRegistryClient.class);
    SchemaMetadata schemaMetadataMock = mock(SchemaMetadata.class);

    expect(schemaRegistryClientMock.getLatestSchemaMetadata("subject1"))
        .andReturn(schemaMetadataMock);
    expect(schemaMetadataMock.getSchemaType()).andReturn(EmbeddedFormat.AVRO.name());
    expect(schemaMetadataMock.getSchema()).andReturn(TextNode.valueOf("schema").toString());
    expect(schemaMetadataMock.getReferences()).andReturn(emptyList());
    replay(schemaRegistryClientMock, schemaMetadataMock);

    SchemaManager mySchemaManager =
        new SchemaManagerImpl(Optional.of(schemaRegistryClientMock), new TopicNameStrategy(), true);

    boolean checkpoint = false;
    try {
      mySchemaManager.getSchema(
          TOPIC_NAME,
          /* format= */ Optional.empty(),
          /* subject= */ Optional.of("subject1"),
          /* subjectNameStrategy= */ Optional.empty(),
          /* schemaId= */ Optional.empty(),
          /* schemaVersion= */ Optional.empty(),
          /* rawSchema= */ Optional.empty(),
          /* isKey= */ true);
    } catch (RestConstraintViolationException rcve) {
      assertEquals(
          "Invalid schema: Error when fetching latest schema version. subject = subject1",
          rcve.getMessage());
      assertEquals(42205, rcve.getErrorCode());
      checkpoint = true;
    }
    assertTrue(checkpoint);
  }

  @Test
  public void errorFetchingLatestSchemaBySchemaVersionBadRequest()
      throws RestClientException, IOException {

    SchemaRegistryClient schemaRegistryClientMock = mock(SchemaRegistryClient.class);
    SchemaMetadata schemaMetadataMock = mock(SchemaMetadata.class);

    expect(schemaRegistryClientMock.getLatestSchemaMetadata("subject1"))
        .andReturn(schemaMetadataMock);
    expect(schemaMetadataMock.getSchemaType())
        .andThrow(
            new UnsupportedOperationException(
                "testing exception")); // this is faking the UnsupportedOperationException but I
    // can't see another way to do this.
    expect(schemaMetadataMock.getSchemaType()).andReturn("schemaType");

    replay(schemaRegistryClientMock, schemaMetadataMock);

    SchemaManager mySchemaManager =
        new SchemaManagerImpl(Optional.of(schemaRegistryClientMock), new TopicNameStrategy(), true);

    boolean checkpoint = false;
    try {
      mySchemaManager.getSchema(
          TOPIC_NAME,
          /* format= */ Optional.empty(),
          /* subject= */ Optional.of("subject1"),
          /* subjectNameStrategy= */ Optional.empty(),
          /* schemaId= */ Optional.empty(),
          /* schemaVersion= */ Optional.empty(),
          /* rawSchema= */ Optional.empty(),
          /* isKey= */ true);
    } catch (BadRequestException bre) {
      assertEquals("Schema subject not supported for schemaType", bre.getMessage());
      assertEquals(400, bre.getCode());
      checkpoint = true;
    }
    assertTrue(checkpoint);
  }

  private static final class MySubjectNameStrategy implements SubjectNameStrategy {

    @Override
    public String subjectName(String topicName, boolean isKey, ParsedSchema schema) {
      return "my-subject-" + topicName + "-" + (isKey ? "key" : "value");
    }

    @Override
    public void configure(Map<String, ?> map) {}
  }

  private static final class SchemaDependentSubjectNameStrategy implements SubjectNameStrategy {

    @Override
    public String subjectName(String topicName, boolean isKey, ParsedSchema schema) {
      if (schema != null) {
        return "my-subject-" + schema.toString();
      } else {
        return "my-subject-";
      }
    }

    @Override
    public void configure(Map<String, ?> map) {}
  }

  private static final class NullReturningSubjectNameStrategy implements SubjectNameStrategy {

    @Override
    public String subjectName(String topicName, boolean isKey, ParsedSchema schema) {
      return null;
    }

    @Override
    public void configure(Map<String, ?> map) {}
  }
}
