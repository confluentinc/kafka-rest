/**
 * Copyright 2015 Confluent Inc.
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

package io.confluent.kafkarest.entities;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

import io.confluent.rest.validation.ConstraintViolations;

public class BinaryConsumerRecord extends ConsumerRecord<byte[], byte[]> {

  @JsonCreator
  public BinaryConsumerRecord(@JsonProperty("topic") String topic,
      @JsonProperty("key") String key, @JsonProperty("value") String value,
      @JsonProperty("partition") int partition, @JsonProperty("offset") long offset
  ) throws IOException {
    super(topic, decodeBinary(key, "key"), decodeBinary(value, "value"), partition, offset);

    try {
      this.value = EntityUtils.parseBase64Binary(value);
    } catch (IllegalArgumentException e) {
      throw ConstraintViolations.simpleException("Record value contains invalid base64 encoding");
    }
  }

  public BinaryConsumerRecord(String topic, byte[] key, byte[] value, int partition, long offset) {
    super(topic, key, value, partition, offset);
  }

  @Override
  @JsonProperty("key")
  public String getJsonKey() {
    if (key == null) {
      return null;
    }
    return EntityUtils.encodeBase64Binary(key);
  }

  @Override
  @JsonProperty("value")
  public String getJsonValue() {
    if (value == null) {
      return null;
    }
    return EntityUtils.encodeBase64Binary(value);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    BinaryConsumerRecord that = (BinaryConsumerRecord) o;
    return partition == that.partition
           && offset == that.offset
           && Objects.equals(topic, that.topic)
           && Arrays.equals(key, that.key)
           && Arrays.equals(value, that.value);
  }

  @Override
  public int hashCode() {
    return Arrays.deepHashCode(new Object[] { topic, key, value, partition, offset});
  }

  private static byte[] decodeBinary(String binary, String field) {
    try {
      return binary == null ? null : EntityUtils.parseBase64Binary(binary);
    } catch (IllegalArgumentException e) {
      throw ConstraintViolations.simpleException("Record " + field
                                                 + " contains invalid base64 encoding");
    }
  }
}
