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

package io.confluent.kafkarest.converters;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.json.JsonSchemaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Provides conversion of JSON to/from JSON Schema.
 */
public final class JsonSchemaConverter implements SchemaConverter {

  private static final Logger log = LoggerFactory.getLogger(JsonSchemaConverter.class);

  private static final ObjectMapper JSON_MAPPER = new ObjectMapper();

  @Override
  public Object toObject(JsonNode value, ParsedSchema parsedSchema) {
    try {
      return JsonSchemaUtils.toObject(value, (JsonSchema) parsedSchema);
    } catch (Exception e) {
      throw new ConversionException("Failed to convert JSON using JSON Schema: " + e.getMessage());
    }
  }

  /**
   * Converts JSON Schema data to their equivalent JsonNode representation.
   *
   * @param value the value to convert
   * @return an object containing the root JsonNode representing the converted object and the size
   *     in bytes of the data when serialized
   */
  @Override
  public JsonNodeAndSize toJson(Object value) {
    try {
      byte[] bytes = JsonSchemaUtils.toJson(value);
      if (bytes == null) {
        return new JsonNodeAndSize(null, 0);
      }
      return new JsonNodeAndSize(JSON_MAPPER.readTree(bytes), bytes.length);
    } catch (IOException e) {
      log.error("Jackson failed to deserialize JSON: ", e);
      throw new ConversionException("Failed to convert JSON Schema to JSON: " + e.getMessage());
    } catch (RuntimeException e) {
      log.error("Unexpected exception converting JSON Schema to JSON: ", e);
      throw new ConversionException("Failed to convert JSON Schema to JSON: " + e.getMessage());
    }
  }
}
