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

package io.confluent.kafkarest.entities.v2;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.protobuf.ByteString;
import io.confluent.kafkarest.entities.ConsumerRecord;
import io.confluent.kafkarest.entities.EntityUtils;
import java.util.Arrays;
import java.util.Objects;
import java.util.StringJoiner;
import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.PositiveOrZero;

public final class BinaryConsumerRecord {

  @NotNull
  @Nullable
  private final String topic;

  @Nullable
  private final byte[] key;

  @Nullable
  private final byte[] value;

  @PositiveOrZero
  @Nullable
  private final Integer partition;

  @PositiveOrZero
  @Nullable
  private final Long offset;

  @JsonCreator
  private BinaryConsumerRecord(
      @JsonProperty("topic") @Nullable String topic,
      @JsonProperty("key") @Nullable byte[] key,
      @JsonProperty("value") @Nullable byte[] value,
      @JsonProperty("partition") @Nullable Integer partition,
      @JsonProperty("offset") @Nullable Long offset) {
    this.topic = topic;
    this.key = key;
    this.value = value;
    this.partition = partition;
    this.offset = offset;
  }

  @JsonProperty
  @Nullable
  public String getTopic() {
    return topic;
  }

  @JsonProperty
  @Nullable
  public String getKey() {
    return key != null ? EntityUtils.encodeBase64Binary(key) : null;
  }

  @JsonProperty
  @Nullable
  public String getValue() {
    return value != null ? EntityUtils.encodeBase64Binary(value) : null;
  }

  @JsonProperty
  @Nullable
  public Integer getPartition() {
    return partition;
  }

  @JsonProperty
  @Nullable
  public Long getOffset() {
    return offset;
  }

  public static BinaryConsumerRecord fromConsumerRecord(
      ConsumerRecord<ByteString, ByteString> record) {
    if (record.getPartition() < 0) {
      throw new IllegalArgumentException();
    }
    if (record.getOffset() < 0) {
      throw new IllegalArgumentException();
    }
    return new BinaryConsumerRecord(
        Objects.requireNonNull(record.getTopic()),
        record.getKey() != null ? record.getKey().toByteArray() : null,
        record.getValue() != null ? record.getValue().toByteArray() : null,
        record.getPartition(),
        record.getOffset());
  }

  public ConsumerRecord<ByteString, ByteString> toConsumerRecord() {
    if (topic == null) {
      throw new IllegalStateException();
    }
    if (partition == null || partition < 0) {
      throw new IllegalStateException();
    }
    if (offset == null || offset < 0) {
      throw new IllegalStateException();
    }
    return ConsumerRecord.create(
        topic,
        key != null ? ByteString.copyFrom(key) : null,
        value != null ? ByteString.copyFrom(value) : null,
        partition,
        offset);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    BinaryConsumerRecord that = (BinaryConsumerRecord) o;
    return Objects.equals(topic, that.topic)
        && Arrays.equals(key, that.key)
        && Arrays.equals(value, that.value)
        && Objects.equals(partition, that.partition)
        && Objects.equals(offset, that.offset);
  }

  @Override
  public int hashCode() {
    int result = Objects.hash(topic, partition, offset);
    result = 31 * result + Arrays.hashCode(key);
    result = 31 * result + Arrays.hashCode(value);
    return result;
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", BinaryConsumerRecord.class.getSimpleName() + "[", "]")
        .add("topic='" + topic + "'")
        .add("key=" + Arrays.toString(key))
        .add("value=" + Arrays.toString(value))
        .add("partition=" + partition)
        .add("offset=" + offset)
        .toString();
  }
}