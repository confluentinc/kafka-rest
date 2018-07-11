/**
 * Copyright 2015 Confluent Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.kafkarest.entities;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.constraints.Min;

public class RawTopicProduceRecord extends RawProduceRecord
        implements TopicProduceRecord<String, String> {

  // When producing to a topic, a partition may be explicitly requested.
  @Min(0)
  protected Integer partition;

  @JsonCreator
  public RawTopicProduceRecord(
          @JsonProperty("key") String key,
          @JsonProperty("value") String value,
          @JsonProperty("partition") Integer partition
  ) {
    super(key, value);
    this.partition = partition;
  }


  @JsonCreator
  public RawTopicProduceRecord(
          @JsonProperty("key") Object key,
          @JsonProperty("value") Object value,
          @JsonProperty("partition") Integer partition
  ) {
    super(key != null ? key.toString() : null, value != null ? value.toString() : null);
    this.partition = partition;
  }

  public RawTopicProduceRecord(String value, Integer partition) {
    this(null, value, partition);
  }

  public RawTopicProduceRecord(Object value, Integer partition) {
    this(null, value != null ? value.toString() : null, partition);
  }

  @Override
  public Integer getPartition() {
    return partition;
  }

  @Override
  public Integer partition() {
    return partition;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }

    RawTopicProduceRecord that = (RawTopicProduceRecord) o;

    return !(partition != null ? !partition.equals(that.partition) : that.partition != null);

  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + (partition != null ? partition.hashCode() : 0);
    return result;
  }
}
