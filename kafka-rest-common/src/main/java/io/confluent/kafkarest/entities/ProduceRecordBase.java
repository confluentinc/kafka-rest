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

public abstract class ProduceRecordBase<K, V> implements ProduceRecord<K, V> {

  protected K key;
  protected V value;

  public ProduceRecordBase(@JsonProperty K key, @JsonProperty V value) {
    this.key = key;
    this.value = value;
  }

  @JsonIgnore
  public K getKey() {
    return key;
  }

  public void setKey(K key) {
    this.key = key;
  }

  @JsonIgnore
  public V getValue() {
    return value;
  }

  public void setValue(V value) {
    this.value = value;
  }

  @Override
  public Integer partition() {
    return null;
  }

  /**
   * Return a JSON-serializable version of the key. This does not need to handle schemas.
   */
  @JsonProperty("key")
  public Object getJsonKey() {
    return key;
  }

  /**
   * Return a JSON-serializable version of the value. This does not need to handle schemas.
   */
  @JsonProperty("value")
  public Object getJsonValue() {
    return value;
  }
}