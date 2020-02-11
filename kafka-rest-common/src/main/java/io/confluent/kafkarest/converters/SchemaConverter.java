/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.kafkarest.converters;

import com.fasterxml.jackson.databind.JsonNode;
import io.confluent.kafka.schemaregistry.ParsedSchema;

/**
 * Provides conversion of JSON to/from an object for a specific schema type.
 */
public interface SchemaConverter {

  Object toObject(JsonNode value, ParsedSchema schema);

  /**
   * Converts data (including primitive types) to their equivalent JsonNode representation.
   *
   * @param value the value to convert
   * @return an object containing the root JsonNode representing the converted object and the size
   *     in bytes of the data when serialized
   */
  JsonNodeAndSize toJson(Object value);

  final class JsonNodeAndSize {

    private final JsonNode json;
    private final long size;

    public JsonNodeAndSize(JsonNode json, long size) {
      this.json = json;
      this.size = size;
    }

    public JsonNode getJson() {
      return json;
    }

    public long getSize() {
      return size;
    }
  }
}
