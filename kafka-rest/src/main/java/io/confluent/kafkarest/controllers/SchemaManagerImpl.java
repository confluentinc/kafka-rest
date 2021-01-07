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

import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;
import static org.glassfish.jersey.internal.guava.Preconditions.checkArgument;

import com.google.auto.value.AutoOneOf;
import com.google.auto.value.AutoValue;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheBuilderSpec;
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
import io.confluent.kafkarest.config.ConfigModule.SchemaCacheSpecConfig;
import io.confluent.kafkarest.entities.EmbeddedFormat;
import io.confluent.kafkarest.entities.RegisteredSchema;
import java.io.IOException;
import java.util.EnumSet;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.function.Supplier;
import javax.inject.Inject;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

final class SchemaManagerImpl implements SchemaManager {

  private static final EnumSet<EmbeddedFormat> VALID_FORMATS =
      EnumSet.of(EmbeddedFormat.AVRO, EmbeddedFormat.JSONSCHEMA, EmbeddedFormat.PROTOBUF);

  private static final Function<String, String> KEY_SUBJECT_FUNCTION =
      topicName -> topicName + "-key";
  private static final Function<String, String> VALUE_SUBJECT_FUNCTION =
      topicName -> topicName + "-value";

  private final LoadingCache<GetSchemaParams, RegisteredSchema> schemaCache;

  @Inject
  SchemaManagerImpl(
      @ProducerConfigs Map<String, Object> producerConfigs,
      @SchemaCacheSpecConfig CacheBuilderSpec schemaCacheSpecConfig) {
    FormatSchemaCacheLoader cacheLoader =
        new FormatSchemaCacheLoader(
            new KeyValueSchemaCacheLoader(
                new SchemaCacheLoader(
                    AvroSchema.TYPE,
                    createSerializer(KafkaAvroSerializer::new, producerConfigs, /* isKey= */ true),
                    createSchemaProvider(AvroSchemaProvider::new, producerConfigs),
                    KEY_SUBJECT_FUNCTION),
                new SchemaCacheLoader(
                    AvroSchema.TYPE,
                    createSerializer(KafkaAvroSerializer::new, producerConfigs, /* isKey= */ false),
                    createSchemaProvider(AvroSchemaProvider::new, producerConfigs),
                    VALUE_SUBJECT_FUNCTION)),
            new KeyValueSchemaCacheLoader(
                new SchemaCacheLoader(
                    JsonSchema.TYPE,
                    createSerializer(
                        KafkaJsonSchemaSerializer::new, producerConfigs, /* isKey= */ true),
                    createSchemaProvider(JsonSchemaProvider::new, producerConfigs),
                    KEY_SUBJECT_FUNCTION),
                new SchemaCacheLoader(
                    JsonSchema.TYPE,
                    createSerializer(
                        KafkaJsonSchemaSerializer::new, producerConfigs, /* isKey= */ false),
                    createSchemaProvider(JsonSchemaProvider::new, producerConfigs),
                    VALUE_SUBJECT_FUNCTION)),
            new KeyValueSchemaCacheLoader(
                new SchemaCacheLoader(
                    ProtobufSchema.TYPE,
                    createSerializer(
                        KafkaProtobufSerializer::new, producerConfigs, /* isKey= */ true),
                    createSchemaProvider(ProtobufSchemaProvider::new, producerConfigs),
                    KEY_SUBJECT_FUNCTION),
                new SchemaCacheLoader(
                    ProtobufSchema.TYPE,
                    createSerializer(
                        KafkaProtobufSerializer::new, producerConfigs, /* isKey= */ false),
                    createSchemaProvider(ProtobufSchemaProvider::new, producerConfigs),
                    VALUE_SUBJECT_FUNCTION)));

    schemaCache = CacheBuilder.from(schemaCacheSpecConfig).build(cacheLoader);
  }

  private static <S extends Serializer<?>> S createSerializer(
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
    try {
      return schemaCache.get(GetSchemaParams.getSchemaByIdParams(format, schemaId, isKey));
    } catch (ExecutionException | UncheckedExecutionException e) {
      throw new SerializationException("Error when loading schema from cache.", e);
    }
  }

  @Override
  public RegisteredSchema parseSchema(
      EmbeddedFormat format, String topicName, String rawSchema, boolean isKey) {
    try {
      return schemaCache.get(
          GetSchemaParams.parseSchemaParams(format, topicName, rawSchema, isKey));
    } catch (ExecutionException | UncheckedExecutionException e) {
      throw new SerializationException("Error when loading schema from cache.", e);
    }
  }

  private static final class FormatSchemaCacheLoader
      extends CacheLoader<GetSchemaParams, RegisteredSchema> {
    private final KeyValueSchemaCacheLoader avroCacheLoader;
    private final KeyValueSchemaCacheLoader jsonschemaCacheLoader;
    private final KeyValueSchemaCacheLoader protobufCacheLoader;

    private FormatSchemaCacheLoader(
        KeyValueSchemaCacheLoader avroCacheLoader,
        KeyValueSchemaCacheLoader jsonschemaCacheLoader,
        KeyValueSchemaCacheLoader protobufCacheLoader
    ) {
      this.avroCacheLoader = requireNonNull(avroCacheLoader);
      this.jsonschemaCacheLoader = requireNonNull(jsonschemaCacheLoader);
      this.protobufCacheLoader = requireNonNull(protobufCacheLoader);
    }

    @Override
    public RegisteredSchema load(GetSchemaParams params) throws Exception {
      switch (params.getFormat()) {
        case AVRO:
          return avroCacheLoader.load(params);

        case JSONSCHEMA:
          return jsonschemaCacheLoader.load(params);

        case PROTOBUF:
          return protobufCacheLoader.load(params);

        default:
          throw new IllegalArgumentException(
              String.format("Unexpected enum constant: %s", params.getFormat()));
      }
    }
  }

  private static final class KeyValueSchemaCacheLoader
      extends CacheLoader<GetSchemaParams, RegisteredSchema> {
    private final SchemaCacheLoader keyCacheLoader;
    private final SchemaCacheLoader valueCacheLoader;

    private KeyValueSchemaCacheLoader(
        SchemaCacheLoader keyCacheLoader, SchemaCacheLoader valueCacheLoader) {
      this.keyCacheLoader = requireNonNull(keyCacheLoader);
      this.valueCacheLoader = requireNonNull(valueCacheLoader);
    }

    @Override
    public RegisteredSchema load(GetSchemaParams params) throws Exception {
      if (params.isKey()) {
        return keyCacheLoader.load(params);
      } else {
        return valueCacheLoader.load(params);
      }
    }
  }

  private static final class SchemaCacheLoader
      extends CacheLoader<GetSchemaParams, RegisteredSchema> {
    private final String schemaType;
    private final AbstractKafkaSchemaSerDe serializer;
    private final SchemaProvider schemaProvider;
    private final Function<String, String> subjectFunction;

    private SchemaCacheLoader(
        String schemaType,
        AbstractKafkaSchemaSerDe serializer,
        SchemaProvider schemaProvider,
        Function<String, String> subjectFunction) {
      this.schemaType = requireNonNull(schemaType);
      this.serializer = requireNonNull(serializer);
      this.schemaProvider = requireNonNull(schemaProvider);
      this.subjectFunction = requireNonNull(subjectFunction);
    }

    @Override
    public RegisteredSchema load(GetSchemaParams params) throws Exception {
      switch (params.getParams().getOperation()) {
        case SCHEMA_ID:
          return getSchemaById(params.getParams().getSchemaId().getSchemaId());

        case RAW_SCHEMA:
          return parseSchema(
              params.getParams().getRawSchema().getTopicName(),
              params.getParams().getRawSchema().getRawSchema());

        default:
          throw new AssertionError(
              String.format("Unexpected enum constant: %s", params.getParams().getOperation()));
      }
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

    private RegisteredSchema parseSchema(String topicName, String rawSchema)
        throws RestClientException, IOException {
      ParsedSchema schema = schemaProvider.parseSchema(rawSchema, emptyList())
          .orElseThrow(() -> new SerializationException("Error when parsing raw schema."));
      int schemaId = serializer.register(subjectFunction.apply(topicName), schema);
      return RegisteredSchema.create(schemaId, schema);
    }
  }

  @AutoValue
  abstract static class GetSchemaParams {

    GetSchemaParams() {
    }

    abstract EmbeddedFormat getFormat();

    abstract SchemaIdOrRawSchema getParams();

    abstract boolean isKey();

    private static GetSchemaParams getSchemaByIdParams(
        EmbeddedFormat format, int schemaId, boolean isKey) {
      checkArgument(VALID_FORMATS.contains(format), "Invalid format: %s", format);
      return new AutoValue_SchemaManagerImpl_GetSchemaParams(
          format, SchemaIdOrRawSchema.schemaId(schemaId), isKey);
    }

    private static GetSchemaParams parseSchemaParams(
        EmbeddedFormat format, String topicName, String rawSchema, boolean isKey) {
      checkArgument(VALID_FORMATS.contains(format), "Invalid format: %s", format);
      return new AutoValue_SchemaManagerImpl_GetSchemaParams(
          format, SchemaIdOrRawSchema.rawSchema(topicName, rawSchema), isKey);
    }
  }

  @AutoOneOf(SchemaIdOrRawSchema.Operation.class)
  abstract static class SchemaIdOrRawSchema {

    enum Operation {
      SCHEMA_ID,
      RAW_SCHEMA
    }

    SchemaIdOrRawSchema() {
    }

    abstract Operation getOperation();

    abstract SchemaId getSchemaId();

    abstract RawSchema getRawSchema();

    private static SchemaIdOrRawSchema schemaId(int schemaId) {
      return AutoOneOf_SchemaManagerImpl_SchemaIdOrRawSchema.schemaId(
          SchemaId.create(schemaId));
    }

    private static SchemaIdOrRawSchema rawSchema(String topicName, String rawSchema) {
      return AutoOneOf_SchemaManagerImpl_SchemaIdOrRawSchema.rawSchema(
          RawSchema.create(topicName, rawSchema));
    }
  }

  @AutoValue
  abstract static class SchemaId {

    SchemaId() {
    }

    abstract int getSchemaId();

    private static SchemaId create(int schemaId) {
      return new AutoValue_SchemaManagerImpl_SchemaId(schemaId);
    }
  }

  @AutoValue
  abstract static class RawSchema {

    RawSchema() {
    }

    abstract String getTopicName();

    abstract String getRawSchema();

    private static RawSchema create(String topicName, String rawSchema) {
      return new AutoValue_SchemaManagerImpl_RawSchema(topicName, rawSchema);
    }
  }
}
