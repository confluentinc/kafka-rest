/**
 * Copyright 2016 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/
package io.confluent.kafkarest.converters;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Provides conversion of byte[] to JSON.
 */
public class JsonConverter {

  private static final Logger log = LoggerFactory.getLogger(JsonConverter.class);

  private static ObjectMapper objectMapper = new ObjectMapper();

  public static Object deserializeJson(byte[] data) {
    try {
      return data == null ? null : objectMapper.readValue(data, Object.class);
    } catch (Exception e) {
      throw new ConversionException("Failed to convert byte[] to JSON: " + e.getMessage());
    }
  }
}
