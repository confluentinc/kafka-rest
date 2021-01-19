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
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.subject.strategy.SubjectNameStrategy;
import io.confluent.kafkarest.entities.EmbeddedFormat;
import io.confluent.kafkarest.entities.RegisteredSchema;
import java.io.IOException;
import javax.inject.Inject;
import org.apache.kafka.common.errors.SerializationException;

final class SchemaManagerImpl implements SchemaManager {
  private final SchemaRegistryClient schemaRegistryClient;
  private final SubjectNameStrategy subjectNameStrategy;

  @Inject
  SchemaManagerImpl(
      SchemaRegistryClient schemaRegistryClient, SubjectNameStrategy subjectNameStrategy) {
    this.schemaRegistryClient = requireNonNull(schemaRegistryClient);
    this.subjectNameStrategy = requireNonNull(subjectNameStrategy);
  }

  @Override
  public RegisteredSchema getSchemaById(EmbeddedFormat format, int schemaId) {
    checkArgument(format.requiresSchema(), "%s does not support schemas.", format);

    ParsedSchema schema;
    try {
      schema = schemaRegistryClient.getSchemaById(schemaId);
    } catch (IOException | RestClientException e) {
      throw new SerializationException("Error when fetching schema by id.", e);
    }

    if (!format.getSchemaProvider().schemaType().equals(schema.schemaType())) {
      throw new SerializationException(
          String.format(
              "Fetched schema has the wrong type. Expected `%s' but got `%s'.",
              format.getSchemaProvider().schemaType(), schema.schemaType()));
    }

    return RegisteredSchema.create(schemaId, schema);
  }

  @Override
  public RegisteredSchema parseSchema(
      EmbeddedFormat format, String topicName, String rawSchema, boolean isKey) {
    checkArgument(format.requiresSchema(), "%s does not support schemas.", format);

    ParsedSchema schema =
        format.getSchemaProvider().parseSchema(rawSchema, /* references= */ emptyList())
            .orElseThrow(() -> new SerializationException("Error when parsing raw schema."));

    String subject = subjectNameStrategy.subjectName(topicName, isKey, schema);

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

    return RegisteredSchema.create(schemaId, schema);
  }
}
