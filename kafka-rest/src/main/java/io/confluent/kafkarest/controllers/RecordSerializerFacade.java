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

import com.fasterxml.jackson.databind.JsonNode;
import com.google.protobuf.ByteString;
import io.confluent.kafkarest.entities.EmbeddedFormat;
import io.confluent.kafkarest.entities.RegisteredSchema;
import java.util.Optional;

/**
 * A facade covering serializers of all supported {@link EmbeddedFormat formats}.
 */
public interface RecordSerializerFacade {

  /**
   * Serializes the given schemaless {@code data} into a {@link ByteString}.
   *
   * <p>The {@code format} argument must be one of {@link EmbeddedFormat#BINARY} or
   * {@link EmbeddedFormat#JSON}. Returns {@link Optional#empty()} if {@code data} {@link
   * JsonNode#isNull() is null}.
   */
  Optional<ByteString> serializeWithoutSchema(
      EmbeddedFormat format, String topicName, boolean isKey, JsonNode data);

  /**
   * Serializes the given schema-ed {@code data} into a {@link ByteString}.
   *
   * <p>The {@code format} argument must be one of {@link EmbeddedFormat#AVRO}, {@link
   * EmbeddedFormat#JSONSCHEMA} or {@link EmbeddedFormat#PROTOBUF}. Returns {@link Optional#empty()}
   * if {@code data} {@link JsonNode#isNull() is null}.
   */
  Optional<ByteString> serializeWithSchema(
      EmbeddedFormat format,
      String topicName,
      Optional<RegisteredSchema> schema,
      JsonNode data,
      boolean isKey);
}
