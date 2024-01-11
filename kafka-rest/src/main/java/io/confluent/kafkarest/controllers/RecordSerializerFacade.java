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
import com.google.protobuf.ByteString;
import io.confluent.kafkarest.entities.EmbeddedFormat;
import io.confluent.kafkarest.entities.RegisteredSchema;
import java.util.Optional;
import javax.inject.Inject;
import javax.inject.Provider;

final class RecordSerializerFacade implements RecordSerializer {

  private final NoSchemaRecordSerializer noSchemaRecordSerializer;
  private final Provider<SchemaRecordSerializer> schemaRecordSerializerProvider;

  @Inject
  RecordSerializerFacade(
      NoSchemaRecordSerializer noSchemaRecordSerializer,
      Provider<SchemaRecordSerializer> schemaRecordSerializerProvider) {
    this.noSchemaRecordSerializer = requireNonNull(noSchemaRecordSerializer);
    this.schemaRecordSerializerProvider = requireNonNull(schemaRecordSerializerProvider);
  }

  @Override
  public Optional<ByteString> serialize(
      EmbeddedFormat format,
      String topicName,
      Optional<RegisteredSchema> schema,
      JsonNode data,
      boolean isKey) {
    if (format.requiresSchema()) {
      return schemaRecordSerializerProvider.get().serialize(format, topicName, schema, data, isKey);
    } else {
      return noSchemaRecordSerializer.serialize(format, data);
    }
  }
}
