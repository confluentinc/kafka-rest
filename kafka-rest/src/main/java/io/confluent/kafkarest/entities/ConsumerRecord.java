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

import com.google.auto.value.AutoValue;

import javax.annotation.Nullable;
import java.util.List;

@AutoValue
public abstract class ConsumerRecord<K, V> {

  ConsumerRecord() {
  }

  public abstract String getTopic();

  @Nullable
  public abstract K getKey();

  @Nullable
  public abstract V getValue();

  public abstract int getPartition();

  public abstract long getOffset();

  @Nullable
  public abstract List<ForwardHeader> getHeaders();

  public static <K, V> ConsumerRecord<K, V> create(
          String topic,
          @Nullable K key, @Nullable V value, int partition, long offset,
          @Nullable List<ForwardHeader> headers) {
    return new AutoValue_ConsumerRecord<>(topic, key, value, partition, offset, headers);
  }
}
