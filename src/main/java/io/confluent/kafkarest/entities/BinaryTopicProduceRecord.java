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

import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.IOException;

import javax.validation.constraints.Min;

public class BinaryTopicProduceRecord extends BinaryProduceRecord
    implements TopicProduceRecord<byte[], byte[]> {

  // When producing to a topic, a partition may be explicitly requested.
  @Min(0)
  protected Integer partition;

  public BinaryTopicProduceRecord(
      @JsonProperty("key") String key,
      @JsonProperty("value") String value,
      @JsonProperty("partition") Integer partition
  ) throws IOException {
    super(key, value);
    this.partition = partition;
  }

  public BinaryTopicProduceRecord(byte[] key, byte[] value, Integer partition) {
    super(key, value);
    this.partition = partition;
  }

  public BinaryTopicProduceRecord(byte[] key, byte[] value) {
    this(key, value, null);
  }

  public BinaryTopicProduceRecord(byte[] value, Integer partition) {
    this(null, value, partition);
  }

  public BinaryTopicProduceRecord(byte[] value) {
    this(null, value, null);
  }

  @Override
  public Integer partition() {
    return partition;
  }

  @Override
  @JsonProperty
  public Integer getPartition() {
    return partition;
  }

  @JsonProperty
  public void setPartition(Integer partition) {
    this.partition = partition;
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

    BinaryTopicProduceRecord that = (BinaryTopicProduceRecord) o;

    if (partition != null ? !partition.equals(that.partition) : that.partition != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + (partition != null ? partition.hashCode() : 0);
    return result;
  }
}
