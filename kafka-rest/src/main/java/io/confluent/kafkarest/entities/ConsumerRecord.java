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

import java.util.Arrays;
import java.util.Objects;
import java.util.StringJoiner;
import javax.annotation.Nullable;

public final class ConsumerRecord<K, V> {

  private final String topic;

  @Nullable
  private final K key;

  @Nullable
  private final V value;

  private final int partition;

  private final long offset;

  public ConsumerRecord(
      String topic, @Nullable K key, @Nullable V value, int partition, long offset) {
    this.topic = Objects.requireNonNull(topic);
    this.key = key;
    this.value = value;
    this.partition = partition;
    this.offset = offset;
  }

  public String getTopic() {
    return topic;
  }

  @Nullable
  public K getKey() {
    return key;
  }

  @Nullable
  public V getValue() {
    return value;
  }

  public int getPartition() {
    return partition;
  }

  public long getOffset() {
    return offset;
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
           && genericEquals(key, that.key)
           && genericEquals(value, that.value);
  }

  private static boolean genericEquals(Object a, Object b) {
    // This is required because both K and V can be byte[], and comparing byte[] with Objects#equal
    // would give the wrong result.
    if (!(a instanceof byte[] && b instanceof byte[])) {
      return Objects.equals(a, b);
    }
    return Arrays.equals((byte[]) a, (byte[]) b);
  }

  @Override
  public int hashCode() {
    int result = Objects.hash(topic, partition, offset);
    result = 31 * result + genericHashCode(key);
    result = 31 * result + genericHashCode(value);
    return result;
  }

  private static int genericHashCode(Object a) {
    // This is required because both K and V can be byte[], and computing hash code of byte[] with
    // Objects#hashCode would give the wrong result.
    if (!(a instanceof byte[])) {
      return Objects.hashCode(a);
    }
    return Arrays.hashCode((byte[]) a);
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", ConsumerRecord.class.getSimpleName() + "[", "]")
        .add("topic='" + topic + "'")
        .add("key=" + key)
        .add("value=" + value)
        .add("partition=" + partition)
        .add("offset=" + offset)
        .toString();
  }
}
