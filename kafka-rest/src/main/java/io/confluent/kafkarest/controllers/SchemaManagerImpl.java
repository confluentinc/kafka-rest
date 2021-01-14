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
import io.confluent.kafkarest.entities.EmbeddedFormat;
import io.confluent.kafkarest.entities.RegisteredSchema;
import java.io.IOException;
import javax.inject.Inject;
import org.apache.kafka.common.errors.SerializationException;

final class SchemaManagerImpl implements SchemaManager {
  private final SchemaRegistryClient schemaRegistryClient;

  @Inject
  SchemaManagerImpl(SchemaRegistryClient schemaRegistryClient) {
    this.schemaRegistryClient = requireNonNull(schemaRegistryClient);
  }

  @Override
  public RegisteredSchema getSchemaById(String subject, int schemaId) {
    ParsedSchema schema;
    try {
      schema = schemaRegistryClient.getSchemaBySubjectAndId(subject, schemaId);
    } catch (IOException | RestClientException e) {
      throw new SerializationException("Error when fetching schema by id.", e);
    }

    int schemaVersion;
    try {
      schemaVersion = schemaRegistryClient.getVersion(subject, schema);
    } catch (IOException | RestClientException e) {
      throw new SerializationException("Error when fetching schema version.", e);
    }

    return RegisteredSchema.create(subject, schemaId, schemaVersion, schema);
  }

  @Override
  public RegisteredSchema getSchemaById(
      SubjectNameStrategy subjectNameStrategy, String topicName, boolean isKey, int schemaId) {
    ParsedSchema schema;
    try {
      schema = schemaRegistryClient.getSchemaById(schemaId);
    } catch (IOException | RestClientException e) {
      throw new SerializationException("Error when fetching schema by id.", e);
    }

    String subject = subjectNameStrategy.subjectName(topicName, isKey, schema);

    int schemaVersion;
    try {
      schemaVersion = schemaRegistryClient.getVersion(subject, schema);
    } catch (IOException | RestClientException e) {
      throw new SerializationException("Error when fetching schema version.", e);
    }

    return RegisteredSchema.create(subject, schemaId, schemaVersion, schema);
  }

  @Override
  public RegisteredSchema getSchemaByVersion(String subject, int schemaVersion) {
    Schema schema =
        schemaRegistryClient.getByVersion(subject, schemaVersion, /* lookupDeletedSchema= */ false);

    ParsedSchema parsedSchema =
        EmbeddedFormat.forSchemaType(schema.getSchemaType())
            .getSchemaProvider()
            .parseSchema(schema.getSchema(), schema.getReferences(), /* isNew= */ false)
            .orElseThrow(
                () -> new SerializationException("Error when fetching schema by version."));

    return RegisteredSchema.create(
        schema.getSubject(), schema.getId(), schemaVersion, parsedSchema);
  }

  public RegisteredSchema parseSchema(EmbeddedFormat format, String subject, String rawSchema) {
    checkArgument(format.requiresSchema(), "%s does not support schemas.", format);

    ParsedSchema schema =
        format.getSchemaProvider()
            .parseSchema(rawSchema, /* references= */ emptyList(), /* isNew= */ true)
            .orElseThrow(() -> new SerializationException("Error when parsing raw schema."));

    return parseSchema(subject, schema);
  }

  @Override
  public RegisteredSchema parseSchema(
      EmbeddedFormat format,
      SubjectNameStrategy schemaSubjectStrategy,
      String topicName,
      boolean isKey,
      String rawSchema) {
    checkArgument(format.requiresSchema(), "%s does not support schemas.", format);

    ParsedSchema schema =
        format.getSchemaProvider()
            .parseSchema(rawSchema, /* references= */ emptyList(), /* isNew= */ true)
            .orElseThrow(() -> new SerializationException("Error when parsing raw schema."));

    String subject = schemaSubjectStrategy.subjectName(topicName, isKey, schema);

    return parseSchema(subject, schema);
  }

  private RegisteredSchema parseSchema(String subject, ParsedSchema schema) {
    int schemaId;
    try {
      try {
        // Check if the schema already exists first.
        schemaId = schemaRegistryClient.getId(subject, schema);
      } catch (IOException | RestClientException e) {
        // Could not find the schema. We try to register the schema in that case.
        schemaId = schemaRegistryClient.register(subject, schema);
      }
    } catch (IOException | RestClientException e) {
      throw new SerializationException(e);
    }

    int schemaVersion;
    try {
      schemaVersion = schemaRegistryClient.getVersion(subject, schema);
    } catch (IOException | RestClientException e) {
      throw new SerializationException("Error when fetching schema version.", e);
    }

    return RegisteredSchema.create(subject, schemaId, schemaVersion, schema);
  }

  @Override
  public RegisteredSchema getLatestSchema(String subject) {
    SchemaMetadata metadata;
    try {
      metadata = schemaRegistryClient.getLatestSchemaMetadata(subject);
    } catch (IOException | RestClientException e) {
      throw new SerializationException("Error when fetching latest schema version.", e);
    }

    ParsedSchema schema =
        EmbeddedFormat.forSchemaType(metadata.getSchemaType())
            .getSchemaProvider()
            .parseSchema(metadata.getSchema(), metadata.getReferences(), /* isNew= */ false)
            .orElseThrow(() -> new SerializationException("Error when parsing raw schema."));

    return RegisteredSchema.create(subject, metadata.getId(), metadata.getVersion(), schema);
  }
}
