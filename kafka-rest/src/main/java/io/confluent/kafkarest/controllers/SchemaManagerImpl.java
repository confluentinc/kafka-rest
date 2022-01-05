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

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.subject.strategy.SubjectNameStrategy;
import io.confluent.kafkarest.Errors;
import io.confluent.kafkarest.config.ConfigModule.SchemaRegistryEnabledConfig;
import io.confluent.kafkarest.entities.EmbeddedFormat;
import io.confluent.kafkarest.entities.RegisteredSchema;
import io.confluent.kafkarest.exceptions.BadRequestException;
import java.io.IOException;
import java.util.Optional;
import javax.inject.Inject;
import org.apache.avro.SchemaParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class SchemaManagerImpl implements SchemaManager {
  private final Optional<SchemaRegistryClient> schemaRegistryClient;
  private final SubjectNameStrategy defaultSubjectNameStrategy;
  private final boolean schemaRegistryEnabled;

  private static final Logger log = LoggerFactory.getLogger(SchemaManagerImpl.class);

  @Inject
  public SchemaManagerImpl(
      Optional<SchemaRegistryClient> schemaRegistryClient,
      SubjectNameStrategy defaultSubjectNameStrategy,
      @SchemaRegistryEnabledConfig Boolean schemaRegistryEnabled) {
    this.schemaRegistryClient = requireNonNull(schemaRegistryClient);
    this.defaultSubjectNameStrategy = requireNonNull(defaultSubjectNameStrategy);
    this.schemaRegistryEnabled = schemaRegistryEnabled;
  }

  @Override
  public Optional<RegisteredSchema> getSchema(
      String topicName,
      Optional<EmbeddedFormat> format,
      Optional<String> subject,
      Optional<SubjectNameStrategy> subjectNameStrategy,
      Optional<Integer> schemaId,
      Optional<Integer> schemaVersion,
      Optional<String> rawSchema,
      boolean isKey) {
    // (subject|subjectNameStrategy)?, schemaId

    if (!schemaRegistryEnabled && !format.isPresent()) {
      throw Errors.invalidPayloadException(
          String.format(
              "Type must be specified when the server is configured without a Schema Registry."));
    }

    if (!schemaRegistryEnabled) {
      return Optional.empty();
    }

    if (schemaId.isPresent()) {
      checkArgument(!format.isPresent());
      checkArgument(!schemaVersion.isPresent());
      checkArgument(!rawSchema.isPresent());
      return Optional.of(
          getSchemaFromSchemaId(topicName, subject, subjectNameStrategy, schemaId.get(), isKey));
    }

    // (subject|subjectNameStrategy)?, schemaVersion
    if (schemaVersion.isPresent()) {
      checkArgument(!format.isPresent());
      checkArgument(!rawSchema.isPresent());
      return Optional.of(
          getSchemaFromSchemaVersion(
              topicName, subject, subjectNameStrategy, schemaVersion.get(), isKey));
    }

    // format, (subject|subjectNameStrategy)?, rawSchema
    if (rawSchema.isPresent()) {
      checkArgument(format.isPresent());
      return Optional.of(
          getSchemaFromRawSchema(
              topicName, format.get(), subject, subjectNameStrategy, rawSchema.get(), isKey));
    }

    // (subject|subjectNameStrategy)?
    checkArgument(!format.isPresent());
    return Optional.of(findLatestSchema(topicName, subject, subjectNameStrategy, isKey));
  }

  private RegisteredSchema getSchemaFromSchemaId(
      String topicName,
      Optional<String> subject,
      Optional<SubjectNameStrategy> subjectNameStrategy,
      int schemaId,
      boolean isKey) {
    ParsedSchema schema;
    try {
      schema = schemaRegistryClient.get().getSchemaById(schemaId);
    } catch (IOException | RestClientException e) {
      throw Errors.messageSerializationException(
          String.format("Error when fetching schema by id. schemaId = %d", schemaId));
    }

    String actualSubject =
        subject.orElse(
            subjectNameStrategy
                .orElse(defaultSubjectNameStrategy)
                .subjectName(topicName, isKey, schema));

    int schemaVersion = getSchemaVersion(actualSubject, schema);

    return RegisteredSchema.create(actualSubject, schemaId, schemaVersion, schema);
  }

  private int getSchemaVersion(String subject, ParsedSchema schema) {
    try {
      return schemaRegistryClient.get().getVersion(subject, schema);
    } catch (IOException | RestClientException e) {
      String schemaString;
      if (schema.canonicalString().equals("\"int\"")) {
        schemaString = "";
      } else {
        schemaString = schema.canonicalString();
      }

      throw Errors.messageSerializationException(
          String.format(
              "Error when fetching schema version. subject = %s, schema = %s",
              subject, schemaString),
          e);
    }
  }

  private RegisteredSchema getSchemaFromSchemaVersion(
      String topicName,
      Optional<String> subject,
      Optional<SubjectNameStrategy> subjectNameStrategy,
      int schemaVersion,
      boolean isKey) {
    String actualSubject =
        subject.orElse(getSchemaSubjectUnsafe(topicName, isKey, subjectNameStrategy));

    Schema schema;
    try {
      schema =
          schemaRegistryClient
              .get()
              .getByVersion(actualSubject, schemaVersion, /* lookupDeletedSchema= */ false);
    } catch (RuntimeException restClientException) {
      throw new BadRequestException(
          String.format(
              "Schema does not exist for subject: %s, version: %s", actualSubject, schemaVersion),
          restClientException);
    }

    ParsedSchema parsedSchema;
    try {
      parsedSchema =
          EmbeddedFormat.forSchemaType(schema.getSchemaType())
              .getSchemaProvider() // throws unsupportedOperationException for bad config
              .parseSchema(schema.getSchema(), schema.getReferences(), /* isNew= */ false)
              .orElseThrow(
                  () ->
                      Errors.invalidSchemaException(
                          String.format(
                              "Error when fetching schema by version. subject = %s, version = %d",
                              actualSubject, schemaVersion)));
    } catch (UnsupportedOperationException | SchemaParseException e) {
      throw new BadRequestException(
          String.format("Schema version not supported for %s", schema.getSchemaType()), e);
    }

    return RegisteredSchema.create(
        schema.getSubject(), schema.getId(), schemaVersion, parsedSchema);
  }

  private RegisteredSchema getSchemaFromRawSchema(
      String topicName,
      EmbeddedFormat format,
      Optional<String> subject,
      Optional<SubjectNameStrategy> subjectNameStrategy,
      String rawSchema,
      boolean isKey) {
    checkArgument(format.requiresSchema(), "%s does not support schemas.", format);

    ParsedSchema schema;
    try {
      schema =
          format
              .getSchemaProvider() // throws UnsupportedOperationException for bad config
              .parseSchema(rawSchema, /* references= */ emptyList(), /* isNew= */ true)
              .orElseThrow(
                  () ->
                      Errors.invalidSchemaException(
                          String.format(
                              "Error when parsing raw schema. format = %s, schema = %s",
                              format, rawSchema)));
    } catch (UnsupportedOperationException | SchemaParseException e) {
      throw new BadRequestException(String.format("Raw schema not supported with %s", format), e);
    }

    String actualSubject =
        subject.orElse(
            subjectNameStrategy
                .orElse(defaultSubjectNameStrategy)
                .subjectName(topicName, isKey, schema));

    int schemaId;
    try {
      try {
        // Check if the schema already exists first.
        schemaId = schemaRegistryClient.get().getId(actualSubject, schema);
      } catch (IOException | RestClientException e) {
        // Could not find the schema. We try to register the schema in that case.
        schemaId = schemaRegistryClient.get().register(actualSubject, schema);
      }
    } catch (IOException | RestClientException e) {
      throw Errors.messageSerializationException(
          String.format(
              "Error when registering schema. format = %s, subject = %s, schema = %s",
              format, actualSubject, schema.canonicalString()),
          e);
    }

    int schemaVersion = getSchemaVersion(actualSubject, schema);

    return RegisteredSchema.create(actualSubject, schemaId, schemaVersion, schema);
  }

  private RegisteredSchema findLatestSchema(
      String topicName,
      Optional<String> subject,
      Optional<SubjectNameStrategy> subjectNameStrategy,
      boolean isKey) {
    String actualSubject =
        subject.orElse(getSchemaSubjectUnsafe(topicName, isKey, subjectNameStrategy));

    SchemaMetadata metadata;
    try {
      metadata = schemaRegistryClient.get().getLatestSchemaMetadata(actualSubject);
    } catch (IOException | RestClientException e) {
      throw Errors.messageSerializationException(
          String.format("Error when fetching latest schema version. subject = %s", actualSubject),
          e);
    }

    ParsedSchema schema;
    try {
      schema =
          EmbeddedFormat.forSchemaType(metadata.getSchemaType())
              .getSchemaProvider() // throws UnsupportedOperationException for bad config
              .parseSchema(metadata.getSchema(), metadata.getReferences(), /* isNew= */ false)
              .orElseThrow(
                  () ->
                      Errors.invalidSchemaException(
                          String.format(
                              "Error when fetching latest schema version. subject = %s",
                              actualSubject)));
    } catch (UnsupportedOperationException | SchemaParseException e) {
      // Shouldn't be possible to get here as "forSchemaType" throws an IllegalArgumentException if
      // the schema type doesn't support fetching the latest schema version
      throw new BadRequestException(
          String.format("Schema subject not supported for %s", metadata.getSchemaType()), e);
    }

    return RegisteredSchema.create(actualSubject, metadata.getId(), metadata.getVersion(), schema);
  }

  /**
   * Tries to get the schema subject from only schema_subject_strategy, {@code topicName} and {@code
   * isKey}.
   *
   * <p>This operation is only really supported if schema_subject_strategy does not depend on the
   * parsed schema to generate the subject name, as we need the subject name to fetch the schema by
   * version. That's the case, for example, of TopicNameStrategy
   * (schema_subject_strategy=TOPIC_NAME). Since TopicNameStrategy is so popular, instead of
   * requiring users to always specify schema_subject if using schema_version?, we try using the
   * strategy to generate the subject name, and fail if that does not work out.
   */
  private String getSchemaSubjectUnsafe(
      String topicName, boolean isKey, Optional<SubjectNameStrategy> subjectNameStrategy) {
    SubjectNameStrategy strategy = subjectNameStrategy.orElse(defaultSubjectNameStrategy);

    String subject = null;
    Exception cause = null;
    try {
      subject = strategy.subjectName(topicName, isKey, /* schema= */ null);
    } catch (Exception e) {
      cause = e;
    }

    if (subject == null) {
      IllegalArgumentException error =
          new IllegalArgumentException(
              String.format(
                  "Cannot use%s schema_subject_strategy%s without schema_id or schema.",
                  subjectNameStrategy.map(requestStrategy -> "").orElse(" default"),
                  subjectNameStrategy.map(requestStrategy -> "=" + strategy).orElse("")));
      if (cause != null) {
        error.initCause(cause);
      }
      throw error;
    }

    return subject;
  }
}
