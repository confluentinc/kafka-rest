/*
 * Copyright 2021 Confluent Inc.
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

import static java.util.Objects.requireNonNull;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.io.BaseEncoding;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaUtils;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.json.JsonSchemaUtils;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaUtils;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaJsonSerializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import io.confluent.kafkarest.KafkaRestConfig;
import io.confluent.kafkarest.config.ConfigModule.ProducerConfigs;
import io.confluent.kafkarest.entities.EmbeddedFormat;
import io.confluent.kafkarest.entities.RegisteredSchema;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import javax.inject.Inject;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serializer;

final class RecordSerializerFacadeImpl implements RecordSerializerFacade {

  private static final ThrowingRecordSerializer NO_SCHEMA_REGISTRY_URL_SERIALIZER =
      new ThrowingRecordSerializer(
          () ->
              new SerializationException(
                  "Missing required configuration \"schema.registry.url\" which has no default "
                      + "value."));

  private final KeyValueRecordSerializer binaryRecordSerializer;
  private final KeyValueRecordSerializer jsonRecordSerializer;
  private final KeyValueRecordSerializer avroRecordSerializer;
  private final KeyValueRecordSerializer jsonschemaRecordSerializer;
  private final KeyValueRecordSerializer protobufRecordSerializer;

  @Inject
  RecordSerializerFacadeImpl(@ProducerConfigs Map<String, Object> producerConfigs) {
    binaryRecordSerializer = createBinaryRecordSerializer(producerConfigs);
    jsonRecordSerializer = createJsonRecordSerializer(producerConfigs);
    avroRecordSerializer = createAvroRecordSerializer(producerConfigs);
    jsonschemaRecordSerializer = createJsonschemaRecordSerializer(producerConfigs);
    protobufRecordSerializer = createProtobufRecordSerializer(producerConfigs);
  }

  private static KeyValueRecordSerializer createBinaryRecordSerializer(
      Map<String, Object> configs) {
    return new KeyValueRecordSerializer(
        new RecordSerializerImpl<>(
            (schema, data) -> BaseEncoding.base64().decode(data.asText()),
            createSerializer(ByteArraySerializer::new, configs, /* isKey= */ true)),
        new RecordSerializerImpl<>(
            (schema, data) -> BaseEncoding.base64().decode(data.asText()),
            createSerializer(ByteArraySerializer::new, configs, /* isKey= */ false)));
  }

  private static KeyValueRecordSerializer createJsonRecordSerializer(
      Map<String, Object> configs) {
    return new KeyValueRecordSerializer(
        new RecordSerializerImpl<>(
            (schema, data) -> data,
            createSerializer(KafkaJsonSerializer::new, configs, /* isKey= */ true)),
        new RecordSerializerImpl<>(
            (schema, data) -> data,
            createSerializer(KafkaJsonSerializer::new, configs, /* isKey= */ false)));
  }

  private static KeyValueRecordSerializer createAvroRecordSerializer(Map<String, Object> configs) {
    if (!configs.containsKey(KafkaRestConfig.SCHEMA_REGISTRY_URL_CONFIG)) {
      return new KeyValueRecordSerializer(
          NO_SCHEMA_REGISTRY_URL_SERIALIZER, NO_SCHEMA_REGISTRY_URL_SERIALIZER);
    }
    return new KeyValueRecordSerializer(
        new RecordSerializerImpl<>(
            (schema, data) ->
                AvroSchemaUtils.toObject(data, (AvroSchema) schema.get().getSchema()),
            createSerializer(KafkaAvroSerializer::new, configs, /* isKey= */ true)),
        new RecordSerializerImpl<>(
            (schema, data) ->
                AvroSchemaUtils.toObject(data, (AvroSchema) schema.get().getSchema()),
            createSerializer(KafkaAvroSerializer::new, configs, /* isKey= */ false)));
  }

  private static KeyValueRecordSerializer createJsonschemaRecordSerializer(
      Map<String, Object> configs) {
    if (!configs.containsKey(KafkaRestConfig.SCHEMA_REGISTRY_URL_CONFIG)) {
      return new KeyValueRecordSerializer(
          NO_SCHEMA_REGISTRY_URL_SERIALIZER, NO_SCHEMA_REGISTRY_URL_SERIALIZER);
    }
    return new KeyValueRecordSerializer(
        new RecordSerializerImpl<>(
            (schema, data) ->
                JsonSchemaUtils.toObject(data, (JsonSchema) schema.get().getSchema()),
            createSerializer(
                KafkaJsonSchemaSerializer::new, configs, /* isKey= */ true)),
        new RecordSerializerImpl<>(
            (schema, data) ->
                JsonSchemaUtils.toObject(data, (JsonSchema) schema.get().getSchema()),
            createSerializer(
                KafkaJsonSchemaSerializer::new, configs, /* isKey= */ false)));
  }

  private static KeyValueRecordSerializer createProtobufRecordSerializer(
      Map<String, Object> configs) {
    if (!configs.containsKey(KafkaRestConfig.SCHEMA_REGISTRY_URL_CONFIG)) {
      return new KeyValueRecordSerializer(
          NO_SCHEMA_REGISTRY_URL_SERIALIZER, NO_SCHEMA_REGISTRY_URL_SERIALIZER);
    }
    return new KeyValueRecordSerializer(
        new RecordSerializerImpl<>(
            (schema, data) ->
                (Message)
                    ProtobufSchemaUtils.toObject(
                        data, (ProtobufSchema) schema.get().getSchema()),
            createSerializer(
                KafkaProtobufSerializer::new, configs, /* isKey= */ true)),
        new RecordSerializerImpl<>(
            (schema, data) ->
                (Message)
                    ProtobufSchemaUtils.toObject(
                        data, (ProtobufSchema) schema.get().getSchema()),
            createSerializer(
                KafkaProtobufSerializer::new, configs, /* isKey= */ false)));
  }

  private static <T, S extends Serializer<T>> S createSerializer(
      Supplier<S> ctor, Map<String, ?> configs, boolean isKey) {
    S serializer = ctor.get();
    serializer.configure(configs, isKey);
    return serializer;
  }

  @Override
  public Optional<ByteString> serializeWithoutSchema(
      EmbeddedFormat format, String topicName, boolean isKey, JsonNode data) {
    switch (format) {
      case BINARY:
        return serializeBinary(topicName, data, isKey);

      case JSON:
        return serializeJson(topicName, data, isKey);

      default:
        throw new IllegalArgumentException(String.format("Format %s not supported.", format));
    }
  }

  private Optional<ByteString> serializeBinary(String topicName, JsonNode data, boolean isKey) {
    return binaryRecordSerializer.serialize(
        topicName, /* schema= */ Optional.empty(), data, isKey);
  }

  private Optional<ByteString> serializeJson(String topicName, JsonNode data, boolean isKey) {
    return jsonRecordSerializer.serialize(
        topicName, /* schema= */ Optional.empty(), data, isKey);
  }

  @Override
  public Optional<ByteString> serializeWithSchema(
      EmbeddedFormat format,
      String topicName,
      Optional<RegisteredSchema> schema,
      JsonNode data,
      boolean isKey) {
    requireNonNull(topicName);
    if (!data.isNull() && !schema.isPresent()) {
      throw new SerializationException(
          String.format(
              "Cannot serialize a non-null %s without a %s schema.",
              isKey ? "key" : "value", isKey ? "key" : "value"));
    }

    switch (format) {
      case AVRO:
        return serializeAvro(topicName, schema, data, isKey);

      case JSONSCHEMA:
        return serializeJsonschema(topicName, schema, data, isKey);

      case PROTOBUF:
        return serializeProtobuf(topicName, schema, data, isKey);

      default:
        throw new IllegalArgumentException(String.format("Format %s not supported.", format));
    }
  }

  private Optional<ByteString> serializeAvro(
      String topicName, Optional<RegisteredSchema> schema, JsonNode data, boolean isKey) {
    return avroRecordSerializer.serialize(topicName, schema, data, isKey);
  }

  private Optional<ByteString> serializeJsonschema(
      String topicName, Optional<RegisteredSchema> schema, JsonNode data, boolean isKey) {
    return jsonschemaRecordSerializer.serialize(topicName, schema, data, isKey);
  }

  private Optional<ByteString> serializeProtobuf(
      String topicName, Optional<RegisteredSchema> schema, JsonNode data, boolean isKey) {
    return protobufRecordSerializer.serialize(topicName, schema, data, isKey);
  }

  private static final class KeyValueRecordSerializer {
    private final RecordSerializer keyRecordSerializer;
    private final RecordSerializer valueRecordSerializer;

    private KeyValueRecordSerializer(
        RecordSerializer keyRecordSerializer, RecordSerializer valueRecordSerializer) {
      this.keyRecordSerializer = requireNonNull(keyRecordSerializer);
      this.valueRecordSerializer = requireNonNull(valueRecordSerializer);
    }

    private Optional<ByteString> serialize(
        String topicName, Optional<RegisteredSchema> schema, JsonNode data, boolean isKey) {
      if (isKey) {
        return serializeKey(topicName, schema, data);
      } else {
        return serializeValue(topicName, schema, data);
      }
    }

    private Optional<ByteString> serializeKey(
        String topicName, Optional<RegisteredSchema> schema, JsonNode data) {
      return keyRecordSerializer.serialize(topicName, schema, data);
    }

    private Optional<ByteString> serializeValue(
        String topicName, Optional<RegisteredSchema> schema, JsonNode data) {
      return valueRecordSerializer.serialize(topicName, schema, data);
    }
  }

  private interface RecordSerializer {

    Optional<ByteString> serialize(
        String topicName, Optional<RegisteredSchema> schema, JsonNode data);
  }

  private static final class RecordSerializerImpl<I> implements RecordSerializer {

    private final RecordParser<? extends I> parser;
    private final Serializer<I> serializer;

    private RecordSerializerImpl(RecordParser<? extends I> parser, Serializer<I> serializer) {
      this.parser = requireNonNull(parser);
      this.serializer = requireNonNull(serializer);
    }

    @Override
    public Optional<ByteString> serialize(
        String topicName, Optional<RegisteredSchema> schema, JsonNode data) {
      if (data.isNull()) {
        return Optional.empty();
      }

      I intermediate;
      try {
        intermediate = parser.parse(schema, data);
      } catch (Exception e) {
        throw new SerializationException("Error when converting data to intermediate format.", e);
      }

      return Optional.of(ByteString.copyFrom(serializer.serialize(topicName, intermediate)));
    }
  }

  private interface RecordParser<I> {

    I parse(Optional<RegisteredSchema> schema, JsonNode node) throws Exception;
  }

  private static final class ThrowingRecordSerializer implements RecordSerializer {

    private final Supplier<? extends SerializationException> throwableSupplier;

    private ThrowingRecordSerializer(Supplier<? extends SerializationException> throwableSupplier) {
      this.throwableSupplier = requireNonNull(throwableSupplier);
    }

    @Override
    public Optional<ByteString> serialize(
        String topicName, Optional<RegisteredSchema> schema, JsonNode data) {
      throw throwableSupplier.get();
    }
  }
}
