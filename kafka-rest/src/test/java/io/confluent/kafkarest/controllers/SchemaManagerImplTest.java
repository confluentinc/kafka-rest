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

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.json.JsonSchemaProvider;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.confluent.kafkarest.entities.EmbeddedFormat;
import io.confluent.kafkarest.entities.RegisteredSchema;
import java.util.Arrays;
import org.apache.kafka.common.errors.SerializationException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class SchemaManagerImplTest {
  private static final String SCHEMA_REGISTRY_SCOPE = "sr";

  private MockSchemaRegistryClient schemaRegistryClient;
  private SchemaManager schemaManager;

  @Before
  public void setUp() {
    schemaRegistryClient =
        (MockSchemaRegistryClient)
            MockSchemaRegistry.getClientForScope(
                SCHEMA_REGISTRY_SCOPE,
                Arrays.asList(
                    new AvroSchemaProvider(),
                    new JsonSchemaProvider(),
                    new ProtobufSchemaProvider()));

    schemaManager = new SchemaManagerImpl(schemaRegistryClient);
  }

  @After
  public void tearDown() {
    schemaRegistryClient.reset();
  }

  @Test
  public void getAvroSchemaById_returnsSchema() throws Exception {
    ParsedSchema schema = new AvroSchema("{\"type\": \"int\"}");
    int schemaId = schemaRegistryClient.register("topic-1-key", schema);

    RegisteredSchema result = schemaManager.getSchemaById("topic-1-key", schemaId);

    assertEquals(schema, result.getSchema());
    assertEquals(schemaId, result.getSchemaId());
  }

  @Test
  public void getJsonschemaSchemaById_returnsSchema() throws Exception {
    ParsedSchema schema = new JsonSchema("{\"type\":\"number\"}");
    int schemaId = schemaRegistryClient.register("topic-1-key", schema);

    RegisteredSchema result = schemaManager.getSchemaById("topic-1-key", schemaId);

    assertEquals(schema, result.getSchema());
    assertEquals(schemaId, result.getSchemaId());
  }

  @Test
  public void getProtobufSchemaById_returnsSchema() throws Exception {
    ParsedSchema schema =
        new ProtobufSchema("syntax = \"proto3\"; message KeyRecord { int32 key = 1; }");
    int schemaId = schemaRegistryClient.register("topic-1-key", schema);

    RegisteredSchema result = schemaManager.getSchemaById("topic-1-key", schemaId);

    assertEquals(schema, result.getSchema());
    assertEquals(schemaId, result.getSchemaId());
  }

  @Test
  public void parseAvroKeySchema_returnsSchema() {
    String rawSchema = "{\"name\":\"int\",\"type\": \"int\"}";
    ParsedSchema schema = new AvroSchema(rawSchema);

    RegisteredSchema result =
        schemaManager.parseSchema(EmbeddedFormat.AVRO, "topic-1-key", rawSchema);

    assertEquals(schema, result.getSchema());
  }

  @Test
  public void parseAvroValueSchema_returnsSchema() {
    String rawSchema = "{\"name\":\"int\",\"type\": \"int\"}";
    ParsedSchema schema = new AvroSchema(rawSchema);

    RegisteredSchema result =
        schemaManager.parseSchema(EmbeddedFormat.AVRO, "topic-1-key", rawSchema);

    assertEquals(schema, result.getSchema());
  }

  @Test
  public void parseJsonschemaKeySchema_returnsSchema() {
    String rawSchema = "{\"type\":\"number\"}";
    ParsedSchema schema = new JsonSchema(rawSchema);

    RegisteredSchema result =
        schemaManager.parseSchema(EmbeddedFormat.JSONSCHEMA, "topic-1-key", rawSchema);

    assertEquals(schema, result.getSchema());
  }

  @Test
  public void parseJsonschemaValueSchema_returnsSchema() {
    String rawSchema = "{\"type\":\"number\"}";
    ParsedSchema schema = new JsonSchema(rawSchema);

    RegisteredSchema result =
        schemaManager.parseSchema(EmbeddedFormat.JSONSCHEMA, "topic-1-key", rawSchema);

    assertEquals(schema, result.getSchema());
  }

  @Test
  public void parseProtobufKeySchema_returnsSchema() {
    String rawSchema = "syntax = \"proto3\"; message KeyRecord { int32 key = 1; }";
    ParsedSchema schema = new ProtobufSchema(rawSchema);

    RegisteredSchema result =
        schemaManager.parseSchema(EmbeddedFormat.PROTOBUF, "topic-1-key", rawSchema);

    assertEquals(schema, result.getSchema());
  }

  @Test
  public void parseProtobufValueSchema_returnsSchema() {
    String rawSchema = "syntax = \"proto3\"; message KeyRecord { int32 key = 1; }";
    ParsedSchema schema = new ProtobufSchema(rawSchema);

    RegisteredSchema result =
        schemaManager.parseSchema(EmbeddedFormat.PROTOBUF, "topic-1-key", rawSchema);

    assertEquals(schema, result.getSchema());
  }

  @Test
  public void parseSchema_registersSchemaAndReturnsId() {
    String rawSchema = "{\"type\": \"int\"}";

    RegisteredSchema parseSchemaResult =
        schemaManager.parseSchema(EmbeddedFormat.AVRO, "topic-1-key", rawSchema);

    RegisteredSchema getSchemaResult =
        schemaManager.getSchemaById("topic-1-key", parseSchemaResult.getSchemaId());

    assertEquals(parseSchemaResult.getSchema(), getSchemaResult.getSchema());
    assertEquals(parseSchemaResult.getSchemaId(), getSchemaResult.getSchemaId());
  }

  @Test
  public void parseSchemaTwiceSameSchema_registersSchemaOnlyOnce() {
    String rawSchema = "{\"type\": \"int\"}";

    RegisteredSchema parseSchemaResult1 =
        schemaManager.parseSchema(EmbeddedFormat.AVRO, "topic-1-key", rawSchema);

    RegisteredSchema parseSchemaResult2 =
        schemaManager.parseSchema(EmbeddedFormat.AVRO, "topic-1-key", rawSchema);

    assertEquals(parseSchemaResult1.getSchema(), parseSchemaResult2.getSchema());
    assertEquals(parseSchemaResult1.getSchemaId(), parseSchemaResult2.getSchemaId());
  }

  @Test(expected = SerializationException.class)
  public void parseSchema_wrongFormat_throwsSerializationException() {
    String rawSchema = "{\"type\": \"int\"}";

    schemaManager.parseSchema(EmbeddedFormat.PROTOBUF, "topic-1=key", rawSchema);
  }

  @Test(expected = IllegalArgumentException.class)
  public void parseSchema_binary_throwsIllegalArgumentException() {
    String rawSchema = "{\"type\": \"int\"}";

    schemaManager.parseSchema(EmbeddedFormat.BINARY, "topic-1-key", rawSchema);
  }

  @Test(expected = IllegalArgumentException.class)
  public void parseSchema_json_throwsIllegalArgumentException() {
    String rawSchema = "{\"type\": \"int\"}";

    schemaManager.parseSchema(EmbeddedFormat.JSON, "topic-1-key", rawSchema);
  }
}
