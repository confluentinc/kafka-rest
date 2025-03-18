/*
 * Copyright 2022 Confluent Inc.
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

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.node.NullNode;
import com.google.protobuf.ByteString;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafkarest.entities.EmbeddedFormat;
import io.confluent.rest.exceptions.RestConstraintViolationException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.easymock.EasyMock;
import org.junit.jupiter.api.Test;

public class SchemaRecordSerializerTest {

  @Test
  public void errorWhenNoSchemaRegistryDefined() {
    SchemaRecordSerializer schemaRecordSerializer = new SchemaRecordSerializerThrowing();
    RestConstraintViolationException rcve =
        assertThrows(
            RestConstraintViolationException.class,
            () ->
                schemaRecordSerializer.serialize(
                    EmbeddedFormat.AVRO, "topic", Optional.empty(), null, true));

    assertEquals(42207, rcve.getErrorCode());
    assertEquals(
        "Error serializing message. Schema Registry not defined, "
            + "no Schema Registry client available to serialize message.",
        rcve.getMessage());
  }

  @Test
  public void errorWhenNullValueInAvroSerialization() {
    SchemaRegistryClient schemaRegistryClient = EasyMock.createNiceMock(SchemaRegistryClient.class);
    Map<String, Object> commonConfigs = new HashMap<>();
    commonConfigs.put("schema.registry.url", "http://localhost:8081");
    SchemaRecordSerializer schemaRecordSerializer =
        new SchemaRecordSerializerImpl(
            schemaRegistryClient, commonConfigs, commonConfigs, commonConfigs);
    RestConstraintViolationException rcve =
        assertThrows(
            RestConstraintViolationException.class,
            () ->
                schemaRecordSerializer.serialize(
                    EmbeddedFormat.AVRO, "topic", Optional.empty(), NullNode.getInstance(), false));

    assertEquals(42206, rcve.getErrorCode());
    assertEquals("Payload error. Null input provided. Data is required.", rcve.getMessage());
  }

  @Test
  public void noErrorForNullKeyInAvroSerialization() {
    SchemaRegistryClient schemaRegistryClient = EasyMock.createNiceMock(SchemaRegistryClient.class);
    Map<String, Object> commonConfigs = new HashMap<>();
    commonConfigs.put("schema.registry.url", "http://localhost:8081");
    SchemaRecordSerializer schemaRecordSerializer =
        new SchemaRecordSerializerImpl(
            schemaRegistryClient, commonConfigs, commonConfigs, commonConfigs);
    Optional<ByteString> serialized =
        schemaRecordSerializer.serialize(
            EmbeddedFormat.AVRO, "topic", Optional.empty(), NullNode.getInstance(), true);

    assertFalse(serialized.isPresent());
  }
}
