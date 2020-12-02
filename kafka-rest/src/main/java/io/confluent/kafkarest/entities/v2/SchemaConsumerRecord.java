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
import com.fasterxml.jackson.databind.JsonNode;
import io.confluent.kafkarest.entities.ConsumerRecord;
import io.confluent.kafkarest.entities.ForwardHeader;

import java.util.List;
import java.util.Objects;
import java.util.StringJoiner;
import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.PositiveOrZero;

public final class SchemaConsumerRecord {

  @NotNull
  @Nullable
  private final String topic;

  @Nullable
  private final JsonNode key;

  @Nullable
  private final JsonNode value;

  @Nullable
  private final List<ForwardHeader> headers;

  @PositiveOrZero
  @Nullable
  private final Integer partition;

  @PositiveOrZero
  @Nullable
  private final Long offset;

  @JsonCreator
  private SchemaConsumerRecord(
      @JsonProperty("topic") @Nullable String topic,
      @JsonProperty("key") @Nullable JsonNode key,
      @JsonProperty("value") @Nullable JsonNode value,
      @JsonProperty("partition") @Nullable Integer partition,
      @JsonProperty("offset") @Nullable Long offset,
      @JsonProperty("headers") @Nullable List<ForwardHeader> headers) {
    this.topic = topic;
    this.key = key;
    this.value = value;
    this.partition = partition;
    this.offset = offset;
    this.headers = headers;
  }

  @JsonProperty
  @Nullable
  public String getTopic() {
    return topic;
  }

  @JsonProperty
  @Nullable
  public JsonNode getKey() {
    return key;
  }

  @JsonProperty
  @Nullable
  public JsonNode getValue() {
    return value;
  }

  @JsonProperty
  @Nullable
  public List<ForwardHeader> getHeaders() {
    return headers;
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

  public static SchemaConsumerRecord fromConsumerRecord(ConsumerRecord<JsonNode, JsonNode> record) {
    if (record.getPartition() < 0) {
      throw new IllegalArgumentException();
    }
    if (record.getOffset() < 0) {
      throw new IllegalArgumentException();
    }
    return new SchemaConsumerRecord(
        Objects.requireNonNull(record.getTopic()),
        record.getKey(),
        record.getValue(),
        record.getPartition(),
        record.getOffset(), record.getHeaders());
  }

  public ConsumerRecord<JsonNode, JsonNode> toConsumerRecord() {
    if (topic == null) {
      throw new IllegalStateException();
    }
    if (partition == null || partition < 0) {
      throw new IllegalStateException();
    }
    if (offset == null || offset < 0) {
      throw new IllegalStateException();
    }
    return ConsumerRecord.create(topic, key, value, partition, offset, headers);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SchemaConsumerRecord that = (SchemaConsumerRecord) o;
    return Objects.equals(topic, that.topic)
        && Objects.equals(key, that.key)
        && Objects.equals(value, that.value)
        && Objects.equals(partition, that.partition)
        && Objects.equals(offset, that.offset);
  }

  @Override
  public int hashCode() {
    return Objects.hash(topic, key, value, partition, offset);
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", SchemaConsumerRecord.class.getSimpleName() + "[", "]")
        .add("topic='" + topic + "'")
        .add("key=" + key)
        .add("value=" + value)
        .add("partition=" + partition)
        .add("offset=" + offset)
        .toString();
  }
}
