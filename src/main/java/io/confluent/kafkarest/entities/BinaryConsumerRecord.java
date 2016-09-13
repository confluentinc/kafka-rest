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

import java.io.IOException;
import java.util.Arrays;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.confluent.rest.validation.ConstraintViolations;

public class BinaryConsumerRecord extends ConsumerRecord<byte[], byte[]> {

  public BinaryConsumerRecord(
      @JsonProperty("key") String key, @JsonProperty("value") String value,
      @JsonProperty("topic") String topic, @JsonProperty("partition") int partition,
      @JsonProperty("offset") long offset
  ) throws IOException {
    super(convertKey(key), convertValue(value), topic, partition, offset);
  }

  public BinaryConsumerRecord(byte[] key, byte[] value, String topic, int partition, long offset) {
    super(key, value, topic, partition, offset);
  }

  private static byte[] convertValue(String value) {
    try {
      return EntityUtils.parseBase64Binary(value);
    } catch (IllegalArgumentException e) {
      throw ConstraintViolations.simpleException("Record value contains invalid base64 encoding");
    }
  }

  private static byte[] convertKey(String key) {
    try {
      if (key != null) {
        return EntityUtils.parseBase64Binary(key);
      } else {
        return null;
      }
    } catch (IllegalArgumentException e) {
      throw ConstraintViolations.simpleException("Record key contains invalid base64 encoding");
    }
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

    if (offset != that.offset) {
      return false;
    }
    if (topic != null ? !topic.equals(that.topic) : that.topic != null) {
      return false;
    }
    if (partition != that.partition) {
      return false;
    }
    if (!Arrays.equals(key, that.key)) {
      return false;
    }
    if (!Arrays.equals(value, that.value)) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int result = key != null ? Arrays.hashCode(key) : 0;
    result = 31 * result + (value != null ? Arrays.hashCode(value) : 0);
    result = 31 * result + (topic != null ? topic.hashCode() : 0);
    result = 31 * result + partition;
    result = 31 * result + (int) (offset ^ (offset >>> 32));
    return result;
  }

}
