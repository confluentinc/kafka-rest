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

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaUtils;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.json.JsonSchemaUtils;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaUtils;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerializer;
import io.confluent.kafka.serializers.json.AbstractKafkaJsonSchemaSerializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import io.confluent.kafkarest.Errors;
import io.confluent.kafkarest.config.ConfigModule.AvroSerializerConfigs;
import io.confluent.kafkarest.config.ConfigModule.JsonschemaSerializerConfigs;
import io.confluent.kafkarest.config.ConfigModule.ProtobufSerializerConfigs;
import io.confluent.kafkarest.entities.EmbeddedFormat;
import io.confluent.kafkarest.entities.RegisteredSchema;
import io.confluent.kafkarest.exceptions.BadRequestException;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import org.apache.avro.AvroTypeException;
import org.everit.json.schema.ValidationException;

final class SchemaRecordSerializerImpl implements SchemaRecordSerializer {

  private final AvroSerializer avroSerializer;
  private final JsonSchemaSerializer jsonschemaSerializer;
  private final ProtobufSerializer protobufSerializer;

  SchemaRecordSerializerImpl(
      SchemaRegistryClient schemaRegistryClient,
      @AvroSerializerConfigs Map<String, Object> avroSerializerConfigs,
      @JsonschemaSerializerConfigs Map<String, Object> jsonschemaSerializerConfigs,
      @ProtobufSerializerConfigs Map<String, Object> protobufSerializerConfigs) {
    requireNonNull(schemaRegistryClient);
    avroSerializer = new AvroSerializer(schemaRegistryClient, avroSerializerConfigs);
    jsonschemaSerializer =
        new JsonSchemaSerializer(schemaRegistryClient, jsonschemaSerializerConfigs);
    protobufSerializer = new ProtobufSerializer(schemaRegistryClient, protobufSerializerConfigs);
  }

  @Override
  public Optional<ByteString> serialize(
      EmbeddedFormat format,
      String topicName,
      Optional<RegisteredSchema> schema,
      JsonNode data,
      boolean isKey) {
    checkArgument(format.requiresSchema());
    if (data.isNull()) {
      return Optional.empty();
    }
    if (!schema.isPresent()) {
      throw isKey ? Errors.keySchemaMissingException() : Errors.valueSchemaMissingException();
    }

    switch (format) {
      case AVRO:
        return Optional.of(serializeAvro(schema.get().getSubject(), schema.get(), data));

      case JSONSCHEMA:
        return Optional.of(serializeJsonschema(schema.get().getSubject(), schema.get(), data));

      case PROTOBUF:
        return Optional.of(
            serializeProtobuf(schema.get().getSubject(), topicName, schema.get(), data, isKey));

      default:
        throw new AssertionError(String.format("Unexpected enum constant: %s", format));
    }
  }

  private ByteString serializeAvro(String subject, RegisteredSchema schema, JsonNode data) {
    AvroSchema avroSchema = (AvroSchema) schema.getSchema();
    Object record;
    try {
      record = AvroSchemaUtils.toObject(data, avroSchema);
    } catch (AvroTypeException | IOException e) {
      throw new BadRequestException(e.getMessage(), e);
    }
    return ByteString.copyFrom(avroSerializer.serialize(subject, avroSchema, record));
  }

  private ByteString serializeJsonschema(String subject, RegisteredSchema schema, JsonNode data) {
    JsonSchema jsonSchema = (JsonSchema) schema.getSchema();
    Object record;
    try {
      record = JsonSchemaUtils.toObject(data, jsonSchema);
    } catch (IOException | ValidationException e) {
      throw new BadRequestException(e.getMessage(), e);
    }
    return ByteString.copyFrom(jsonschemaSerializer.serialize(subject, jsonSchema, record));
  }

  private ByteString serializeProtobuf(
      String subject, String topicName, RegisteredSchema schema, JsonNode data, boolean isKey) {
    ProtobufSchema protobufSchema = (ProtobufSchema) schema.getSchema();
    Message record;
    try {
      record = (Message) ProtobufSchemaUtils.toObject(data, protobufSchema);
    } catch (IOException e) {
      throw new BadRequestException(e.getMessage(), e);
    }
    return ByteString.copyFrom(
        protobufSerializer.serialize(subject, topicName, protobufSchema, record, isKey));
  }

  private static final class AvroSerializer extends AbstractKafkaAvroSerializer {

    private AvroSerializer(SchemaRegistryClient schemaRegistryClient, Map<String, Object> configs) {
      this.schemaRegistry = requireNonNull(schemaRegistryClient);
      configure(serializerConfig(configs));
    }

    private byte[] serialize(String subject, AvroSchema schema, Object data) {
      return serializeImpl(subject, data, schema);
    }
  }

  private static final class JsonSchemaSerializer
      extends AbstractKafkaJsonSchemaSerializer<Object> {

    private JsonSchemaSerializer(
        SchemaRegistryClient schemaRegistryClient, Map<String, Object> configs) {
      this.schemaRegistry = requireNonNull(schemaRegistryClient);
      configure(serializerConfig(configs));
    }

    private byte[] serialize(String subject, JsonSchema schema, Object data) {
      return serializeImpl(subject, JsonSchemaUtils.getValue(data), schema);
    }
  }

  private static final class ProtobufSerializer extends KafkaProtobufSerializer<Message> {

    private ProtobufSerializer(
        SchemaRegistryClient schemaRegistryClient, Map<String, Object> configs) {
      this.schemaRegistry = requireNonNull(schemaRegistryClient);
      configure(serializerConfig(configs));
    }

    private byte[] serialize(
        String subject, String topicName, ProtobufSchema schema, Message data, boolean isKey) {
      return serializeImpl(subject, topicName, isKey, data, schema);
    }
  }
}
