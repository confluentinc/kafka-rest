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

import io.confluent.kafka.serializers.subject.strategy.SubjectNameStrategy;
import io.confluent.kafkarest.entities.EmbeddedFormat;
import io.confluent.kafkarest.entities.RegisteredSchema;

/**
 * A manager for Schema Registry {@link io.confluent.kafka.schemaregistry.ParsedSchema schemas}.
 */
public interface SchemaManager {

  /**
   * Returns the {@link RegisteredSchema schema} registered with the given {@code schemaId}.
   */
  RegisteredSchema getSchemaById(String subject, int schemaId);

  RegisteredSchema getSchemaById(
      SubjectNameStrategy subjectNameStrategy, String topicName, boolean isKey, int schemaId);

  /**
   * Returns the {@link RegisteredSchema schema} registered with the given {@code schemaVersion}.
   */
  RegisteredSchema getSchemaByVersion(String subject, int schemaVersion);

  /**
   * Parses and returns {@code rawSchema}.
   *
   * <p>If the subject does not contain a registered schema equals to the parsed {@code rawSchema},
   * one will be registered.
   */
  RegisteredSchema parseSchema(EmbeddedFormat format, String subject, String rawSchema);

  RegisteredSchema parseSchema(
      EmbeddedFormat format,
      SubjectNameStrategy subjectNameStrategy,
      String topicName,
      boolean isKey,
      String rawSchema);

  RegisteredSchema getLatestSchema(String subject);
}
