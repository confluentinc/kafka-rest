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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

public abstract class ConsumerRecord<K, V> {

  protected String topic;

  protected K key;
  @NotNull
  protected V value;

  @Min(0)
  protected int partition;

  @Min(0)
  protected long offset;

  public ConsumerRecord(String topic, K key, V value, int partition, long offset) {
    this.topic = topic;
    this.key = key;
    this.value = value;
    this.partition = partition;
    this.offset = offset;
  }

  @JsonProperty
  public String getTopic() {
    return topic;
  }

  @JsonProperty
  public void setTopic(String topic) {
    this.topic = topic;
  }

  @JsonIgnore
  public K getKey() {
    return key;
  }

  @JsonProperty("key")
  public Object getJsonKey() {
    return key;
  }

  @JsonIgnore
  public void setKey(K key) {
    this.key = key;
  }

  @JsonIgnore
  public V getValue() {
    return value;
  }

  @JsonProperty("value")
  public Object getJsonValue() {
    return value;
  }

  @JsonIgnore
  public void setValue(V value) {
    this.value = value;
  }

  @JsonProperty
  public int getPartition() {
    return partition;
  }

  @JsonProperty
  public void setPartition(int partition) {
    this.partition = partition;
  }

  @JsonProperty
  public long getOffset() {
    return offset;
  }

  @JsonProperty
  public void setOffset(int offset) {
    this.offset = offset;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ConsumerRecord<?, ?> that = (ConsumerRecord<?, ?>) o;
    return partition == that.partition
           && offset == that.offset
           && Objects.equals(topic, that.topic)
           && Objects.equals(key, that.key)
           && Objects.equals(value, that.value);
  }

  @Override
  public int hashCode() {
    return Objects.hash(topic, key, value, partition, offset);
  }

}
