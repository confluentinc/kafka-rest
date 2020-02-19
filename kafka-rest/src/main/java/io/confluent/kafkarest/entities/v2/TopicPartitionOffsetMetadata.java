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

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;
import java.util.StringJoiner;
import javax.annotation.Nullable;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.PositiveOrZero;

public final class TopicPartitionOffsetMetadata {

  @NotEmpty
  @Nullable
  private final String topic;

  @PositiveOrZero
  @Nullable
  private final Integer partition;

  @PositiveOrZero
  @Nullable
  private final Long offset;

  @Nullable
  private final String metadata;

  public TopicPartitionOffsetMetadata(
      @JsonProperty("topic") @Nullable String topic,
      @JsonProperty("partition") @Nullable Integer partition,
      @JsonProperty("offset") @Nullable Long offset,
      @JsonProperty("metadata") @Nullable String metadata
  ) {
    this.topic = topic;
    this.partition = partition;
    this.offset = offset;
    this.metadata = metadata;
  }

  @JsonProperty
  @Nullable
  public String getTopic() {
    return topic;
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

  @JsonProperty
  @Nullable
  public String getMetadata() {
    return metadata;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    TopicPartitionOffsetMetadata that = (TopicPartitionOffsetMetadata) o;
    return Objects.equals(topic, that.topic)
        && Objects.equals(partition, that.partition)
        && Objects.equals(offset, that.offset)
        && Objects.equals(metadata, that.metadata);
  }

  @Override
  public int hashCode() {
    return Objects.hash(topic, partition, offset, metadata);
  }

  @Override
  public String toString() {
    return new StringJoiner(
        ", ", TopicPartitionOffsetMetadata.class.getSimpleName() + "[", "]")
        .add("topic='" + topic + "'")
        .add("partition=" + partition)
        .add("offset=" + offset)
        .add("metadata='" + metadata + "'")
        .toString();
  }
}
