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

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.io.BaseEncoding;
import com.google.protobuf.ByteString;
import io.confluent.kafka.serializers.KafkaJsonSerializer;
import io.confluent.kafka.serializers.KafkaJsonSerializerConfig;
import io.confluent.kafkarest.config.ConfigModule.JsonSerializerConfigs;
import io.confluent.kafkarest.entities.EmbeddedFormat;
import io.confluent.kafkarest.exceptions.BadRequestException;
import java.util.Map;
import java.util.Optional;
import javax.inject.Inject;

final class NoSchemaRecordSerializer {

  private final JsonSerializer jsonSerializer;

  @Inject
  NoSchemaRecordSerializer(@JsonSerializerConfigs Map<String, Object> jsonSerializerConfigs) {
    jsonSerializer = new JsonSerializer(jsonSerializerConfigs);
  }

  Optional<ByteString> serialize(EmbeddedFormat format, JsonNode data) {
    checkArgument(!format.requiresSchema());

    if (data.isNull()) {
      return Optional.empty();
    }

    switch (format) {
      case BINARY:
        return Optional.of(serializeBinary(data));

      case JSON:
        return Optional.of(serializeJson(data));

      default:
        throw new AssertionError(String.format("Unexpected enum constant: %s", format));
    }
  }

  private static ByteString serializeBinary(JsonNode data) {
    if (!data.isTextual()) {
      throw new BadRequestException(String.format("data=%s is not a base64 string.", data));
    }
    byte[] serialized;
    try {
      serialized = BaseEncoding.base64().decode(data.asText());
    } catch (IllegalArgumentException e) {
      throw new BadRequestException(
          String.format("data=%s is not a valid base64 string.", data), e);
    }
    return ByteString.copyFrom(serialized);
  }

  private ByteString serializeJson(JsonNode data) {
    return ByteString.copyFrom(jsonSerializer.serialize(data));
  }

  private static final class JsonSerializer extends KafkaJsonSerializer<JsonNode> {

    private JsonSerializer(Map<String, Object> configs) {
      configure(new KafkaJsonSerializerConfig(configs));
    }

    private byte[] serialize(JsonNode data) {
      return serialize(/* topic= */ "", data);
    }
  }
}
