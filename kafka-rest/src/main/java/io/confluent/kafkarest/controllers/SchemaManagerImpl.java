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
import static java.util.Objects.requireNonNull;

import com.google.auto.value.AutoValue;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.UncheckedExecutionException;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.SchemaProvider;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.json.JsonSchemaProvider;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDe;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import io.confluent.kafkarest.config.ConfigModule.ProducerConfigs;
import io.confluent.kafkarest.entities.EmbeddedFormat;
import io.confluent.kafkarest.entities.RegisteredSchema;
import java.io.IOException;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.function.Supplier;
import javax.inject.Inject;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

final class SchemaManagerImpl implements SchemaManager {

  private static final Function<String, String> KEY_SUBJECT_FUNCTION =
      topicName -> topicName + "-key";
  private static final Function<String, String> VALUE_SUBJECT_FUNCTION =
      topicName -> topicName + "-value";

  private final KeyValueSchemaManager avroSchemaManager;
  private final KeyValueSchemaManager jsonschemaSchemaManager;
  private final KeyValueSchemaManager protobufSchemaManager;

  @Inject
  SchemaManagerImpl(@ProducerConfigs Map<String, Object> producerConfigs) {
    this.avroSchemaManager =
        new KeyValueSchemaManager(
            new SchemaManagerInternal(
                AvroSchema.TYPE,
                createSerializer(KafkaAvroSerializer::new, producerConfigs, /* isKey= */ true),
                createSchemaProvider(AvroSchemaProvider::new, producerConfigs),
                KEY_SUBJECT_FUNCTION),
            new SchemaManagerInternal(
                AvroSchema.TYPE,
                createSerializer(KafkaAvroSerializer::new, producerConfigs, /* isKey= */ false),
                createSchemaProvider(AvroSchemaProvider::new, producerConfigs),
                VALUE_SUBJECT_FUNCTION));
    this.jsonschemaSchemaManager =
        new KeyValueSchemaManager(
            new SchemaManagerInternal(
                JsonSchema.TYPE,
                createSerializer(
                    KafkaJsonSchemaSerializer::new, producerConfigs, /* isKey= */ true),
                createSchemaProvider(JsonSchemaProvider::new, producerConfigs),
                KEY_SUBJECT_FUNCTION),
            new SchemaManagerInternal(
                JsonSchema.TYPE,
                createSerializer(
                    KafkaJsonSchemaSerializer::new, producerConfigs, /* isKey= */ false),
                createSchemaProvider(JsonSchemaProvider::new, producerConfigs),
                VALUE_SUBJECT_FUNCTION));
    this.protobufSchemaManager =
        new KeyValueSchemaManager(
            new SchemaManagerInternal(
                ProtobufSchema.TYPE,
                createSerializer(
                    KafkaProtobufSerializer::new, producerConfigs, /* isKey= */ true),
                createSchemaProvider(ProtobufSchemaProvider::new, producerConfigs),
                KEY_SUBJECT_FUNCTION),
            new SchemaManagerInternal(
                ProtobufSchema.TYPE,
                createSerializer(
                    KafkaProtobufSerializer::new, producerConfigs, /* isKey= */ false),
                createSchemaProvider(ProtobufSchemaProvider::new, producerConfigs),
                VALUE_SUBJECT_FUNCTION));
  }

  private static <T, S extends AbstractKafkaSchemaSerDe & Serializer<T>> S createSerializer(
      Supplier<S> ctor, Map<String, ?> configs, boolean isKey) {
    S serializer = ctor.get();
    serializer.configure(configs, isKey);
    return serializer;
  }

  private static SchemaProvider createSchemaProvider(
      Supplier<SchemaProvider> ctor, Map<String, ?> configs) {
    SchemaProvider schemaProvider = ctor.get();
    schemaProvider.configure(configs);
    return schemaProvider;
  }

  @Override
  public RegisteredSchema getSchemaById(EmbeddedFormat format, int schemaId, boolean isKey) {
    switch (format) {
      case AVRO:
        return getAvroSchemaById(schemaId, isKey);

      case JSONSCHEMA:
        return getJsonschemaSchemaById(schemaId, isKey);

      case PROTOBUF:
        return getProtobufSchemaById(schemaId, isKey);

      default:
        throw new IllegalArgumentException(String.format("Format %s not supported.", format));
    }
  }

  private RegisteredSchema getAvroSchemaById(int schemaId, boolean isKey) {
    return avroSchemaManager.getSchemaById(schemaId, isKey);
  }

  private RegisteredSchema getJsonschemaSchemaById(int schemaId, boolean isKey) {
    return jsonschemaSchemaManager.getSchemaById(schemaId, isKey);
  }

  private RegisteredSchema getProtobufSchemaById(int schemaId, boolean isKey) {
    return protobufSchemaManager.getSchemaById(schemaId, isKey);
  }

  @Override
  public RegisteredSchema parseSchema(
      EmbeddedFormat format, String topicName, String rawSchema, boolean isKey) {
    switch (format) {
      case AVRO:
        return parseAvroSchema(topicName, rawSchema, isKey);

      case JSONSCHEMA:
        return parseJsonschemaSchema(topicName, rawSchema, isKey);

      case PROTOBUF:
        return parseProtobufSchema(topicName, rawSchema, isKey);

      default:
        throw new IllegalArgumentException(String.format("Format %s not supported.", format));
    }
  }

  private RegisteredSchema parseAvroSchema(String topicName, String rawSchema, boolean isKey) {
    return avroSchemaManager.parseSchema(topicName, rawSchema, isKey);
  }

  private RegisteredSchema parseJsonschemaSchema(
      String topicName, String rawSchema, boolean isKey) {
    return jsonschemaSchemaManager.parseSchema(topicName, rawSchema, isKey);
  }

  private RegisteredSchema parseProtobufSchema(String topicName, String rawSchema, boolean isKey) {
    return protobufSchemaManager.parseSchema(topicName, rawSchema, isKey);
  }

  private static final class KeyValueSchemaManager {
    private final SchemaManagerInternal keySchemaManager;
    private final SchemaManagerInternal valueSchemaManager;

    private KeyValueSchemaManager(
        SchemaManagerInternal keySchemaManager, SchemaManagerInternal valueSchemaManager) {
      this.keySchemaManager = requireNonNull(keySchemaManager);
      this.valueSchemaManager = requireNonNull(valueSchemaManager);
    }

    private RegisteredSchema getSchemaById(int schemaId, boolean isKey) {
      if (isKey) {
        return getKeySchemaById(schemaId);
      } else {
        return getValueSchemaById(schemaId);
      }
    }

    private RegisteredSchema getKeySchemaById(int schemaId) {
      return keySchemaManager.getSchemaById(schemaId);
    }

    private RegisteredSchema getValueSchemaById(int schemaId) {
      return valueSchemaManager.getSchemaById(schemaId);
    }

    private RegisteredSchema parseSchema(String topicName, String rawSchema, boolean isKey) {
      if (isKey) {
        return parseKeySchema(topicName, rawSchema);
      } else {
        return parseValueSchema(topicName, rawSchema);
      }
    }

    private RegisteredSchema parseKeySchema(String topicName, String rawSchema) {
      return keySchemaManager.parseSchema(topicName, rawSchema);
    }

    private RegisteredSchema parseValueSchema(String topicName, String rawSchema) {
      return valueSchemaManager.parseSchema(topicName, rawSchema);
    }
  }

  private static final class SchemaManagerInternal {
    private final String schemaType;
    private final AbstractKafkaSchemaSerDe serializer;
    private final LoadingCache<ParseSchemaParams, RegisteredSchema> schemaCache;

    private SchemaManagerInternal(
        String schemaType,
        AbstractKafkaSchemaSerDe serializer,
        SchemaProvider schemaProvider,
        Function<String, String> subjectFunction) {
      this.schemaType = requireNonNull(schemaType);
      this.serializer = requireNonNull(serializer);
      schemaCache =
          CacheBuilder.newBuilder()
              .maximumSize(1000)
              .expireAfterAccess(Duration.ofMinutes(10))
              .build(new SchemaCacheLoader(serializer, schemaProvider, subjectFunction));
    }

    private RegisteredSchema getSchemaById(int schemaId) {
      ParsedSchema schema;
      try {
        schema = serializer.getSchemaById(schemaId);
      } catch (IOException | RestClientException e) {
        throw new SerializationException("Error when fetching schema by id.", e);
      }
      if (!schemaType.equals(schema.schemaType())) {
        throw new SerializationException(
            String.format(
                "Fetched schema has the wrong type. Expected `%s' but got `%s'.",
                schemaType,
                schema.schemaType()));
      }
      return RegisteredSchema.create(schemaId, schema);
    }

    private RegisteredSchema parseSchema(String topicName, String rawSchema) {
      try {
        return schemaCache.get(ParseSchemaParams.create(topicName, rawSchema));
      } catch (ExecutionException | UncheckedExecutionException e) {
        throw new SerializationException("Error when loading schema from cache.", e);
      }
    }
  }

  private static final class SchemaCacheLoader
      extends CacheLoader<ParseSchemaParams, RegisteredSchema> {
    private final AbstractKafkaSchemaSerDe serializer;
    private final SchemaProvider schemaProvider;
    private final Function<String, String> subjectFunction;

    private SchemaCacheLoader(
        AbstractKafkaSchemaSerDe serializer,
        SchemaProvider schemaProvider,
        Function<String, String> subjectFunction) {
      this.serializer = requireNonNull(serializer);
      this.schemaProvider = requireNonNull(schemaProvider);
      this.subjectFunction = requireNonNull(subjectFunction);
    }

    @Override
    public RegisteredSchema load(ParseSchemaParams params) throws Exception {
      ParsedSchema schema =
          schemaProvider.parseSchema(params.getRawSchema(), emptyList())
              .orElseThrow(() -> new SerializationException("Error when parsing raw schema."));
      int schemaId = serializer.register(subjectFunction.apply(params.getTopicName()), schema);
      return RegisteredSchema.create(schemaId, schema);
    }
  }

  @AutoValue
  abstract static class ParseSchemaParams {

    ParseSchemaParams() {
    }

    abstract String getTopicName();

    abstract String getRawSchema();

    private static ParseSchemaParams create(String topicName, String rawSchema) {
      return new AutoValue_SchemaManagerImpl_ParseSchemaParams(topicName, rawSchema);
    }
  }
}
