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
import java.util.Optional;

/**
 * A manager for Schema Registry {@link RegisteredSchema schemas}.
 */
public interface SchemaManager {

  /**
   * Returns a {@link RegisteredSchema schema} matching the parameter options.
   *
   * <p>There are two pieces of information required to get an schema: the subject to which the
   * schema is/should be registered, and a schema identifier (or schema itself).
   *
   * <p>The first bit is handled by {@code subject} and {@code subjectNameStrategy}, which are
   * mutually exclusive. If {@code subject} is passed, that's the subject used. If {@code
   * subjectNameStrategy} is passed instead, then it will be used to generate the subject. All
   * strategies (TOPIC_NAME, RECORD_NAME and TOPIC_RECORD_NAME) are valid if using {@code schemaId}
   * or {@code rawSchema}, but only TOPIC_NAME is valid for everything else. If neither {@code
   * subject} or {@code subjectNameStrategy} are passed, a default strategy is used based off the
   * configs {@code schema.registry.key.subject.name.strategy} and {@code
   * schema.registry.value.subject.name.strategy}. The same considerations above apply for which
   * default strategies are valid.
   *
   * <p>The second bit is handled by {@code schemaId}, {@code schemaVersion} and {@code rawSchema},
   * which are mutually exclusive. If {@code schemaId} is passed, that schema is going to be used,
   * but only if the subject (see previous paragraph) contains a version mapped to that schema ID.
   * If {@code schemaVersion} is used, then that version of subject is going to be used. If {@code
   * rawSchema} is used, a new version with that schema is going to be registered in the subject,
   * unless the subject already has a version with exactly the same schema, in which case no new
   * version is registered and that version is used instead. If neither {@code schemaId}, {@code
   * schemaVersion} or {@code rawSchema} are passed, the latest version of the subject is used.
   *
   * <p>If passing {@code rawSchema}, {@code format} is mandatory. {@code format} is otherwise
   * illegal to be passed.
   *
   * <p>Schema Registry is not very descriptive as for error causes, so non-existing subject, schema
   * ID or schema version will result in {@link
   * org.apache.kafka.common.errors.SerializationException}, as it will any other Schema Registry
   * related error. Invalid combination of options will result in {@link IllegalArgumentException}.
   */
  RegisteredSchema getSchema(
      String topicName,
      Optional<EmbeddedFormat> format,
      Optional<String> subject,
      Optional<SubjectNameStrategy> subjectNameStrategy,
      Optional<Integer> schemaId,
      Optional<Integer> schemaVersion,
      Optional<String> rawSchema,
      boolean isKey);
}
