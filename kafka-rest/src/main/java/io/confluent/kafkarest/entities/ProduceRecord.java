/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.kafkarest.entities;

import javax.annotation.Nullable;

public final class ProduceRecord<K, V> {

  @Nullable
  private final K key;

  @Nullable
  private final V value;

  @Nullable
  private final Integer partition;

  public ProduceRecord(@Nullable K key, @Nullable V value, @Nullable Integer partition) {
    this.key = key;
    this.value = value;
    this.partition = partition;
  }

  @Nullable
  public K getKey() {
    return key;
  }

  @Nullable
  public V getValue() {
    return value;
  }

  @Nullable
  public Integer getPartition() {
    return partition;
  }
}