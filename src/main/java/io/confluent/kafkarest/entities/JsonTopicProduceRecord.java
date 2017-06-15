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

import javax.validation.constraints.Min;

public class JsonTopicProduceRecord extends JsonProduceRecord
    implements TopicProduceRecord<Object, Object> {

  // When producing to a topic, a partition may be explicitly requested.
  @Min(0)
  protected Integer partition;

  @JsonCreator
  public JsonTopicProduceRecord(
      @JsonProperty("key") Object key,
      @JsonProperty("value") Object value,
      @JsonProperty("partition") Integer partition
  ) {
    super(key, value);
    this.partition = partition;
  }

  public JsonTopicProduceRecord(Object value, Integer partition) {
    this(null, value, partition);
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

    JsonTopicProduceRecord that = (JsonTopicProduceRecord) o;

    return !(partition != null ? !partition.equals(that.partition) : that.partition != null);

  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + (partition != null ? partition.hashCode() : 0);
    return result;
  }
}
