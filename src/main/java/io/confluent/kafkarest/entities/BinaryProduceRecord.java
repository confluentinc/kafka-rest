/*
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

import io.confluent.rest.validation.ConstraintViolations;

public class BinaryProduceRecord extends ProduceRecordBase<byte[], byte[]> {

  @JsonCreator
  public BinaryProduceRecord(@JsonProperty("key") String key, @JsonProperty("value") String value)
      throws IOException {
    super(null, null);
    try {
      this.key = (key != null) ? EntityUtils.parseBase64Binary(key) : null;
    } catch (IllegalArgumentException e) {
      throw ConstraintViolations.simpleException("Record key contains invalid base64 encoding");
    }
    try {
      this.value = (value != null) ? EntityUtils.parseBase64Binary(value) : null;
    } catch (IllegalArgumentException e) {
      throw ConstraintViolations.simpleException("Record value contains invalid base64 encoding");
    }
  }

  public BinaryProduceRecord(byte[] key, byte[] value) {
    super(key, value);
  }

  public BinaryProduceRecord(byte[] unencodedValue) {
    this(null, unencodedValue);
  }

  @Override
  @JsonProperty("key")
  public String getJsonKey() {
    return (key == null ? null : EntityUtils.encodeBase64Binary(key));
  }

  @Override
  @JsonProperty("value")
  public String getJsonValue() {
    return (value == null ? null : EntityUtils.encodeBase64Binary(value));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    BinaryProduceRecord that = (BinaryProduceRecord) o;

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
    return result;
  }
}
