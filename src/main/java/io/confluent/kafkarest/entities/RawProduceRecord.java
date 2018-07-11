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

public class RawProduceRecord extends ProduceRecordBase<String, String> {

  @JsonCreator
  public RawProduceRecord(
          @JsonProperty("key") String key,
          @JsonProperty("value") String value
  ) {
    super(key, value);
  }

  @JsonCreator
  public RawProduceRecord(
          @JsonProperty("key") Object key,
          @JsonProperty("value") Object value
  ) {
    super(key != null ? key.toString() : null, value != null ? value.toString() : null);
  }

  public RawProduceRecord(String value) {
    this(null, value);
  }

  public RawProduceRecord(Object value) {
    this(null, value != null ? value.toString() : null);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    RawProduceRecord that = (RawProduceRecord) o;

    return key != null
            ? key.equals(that.key)
            : that.key == null && !(value != null ? !value.equals(that.value) : that.value != null);

  }

  @Override
  public int hashCode() {
    int result = key != null ? key.hashCode() : 0;
    result = 31 * result + (value != null ? value.hashCode() : 0);
    return result;
  }
}
