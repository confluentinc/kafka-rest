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
import com.fasterxml.jackson.annotation.JsonValue;
import io.confluent.kafkarest.entities.TopicPartitionOffset;
import java.util.List;
import java.util.Objects;
import java.util.StringJoiner;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.PositiveOrZero;

public final class CommitOffsetsResponse {

  @Nullable
  private final List<Offset> offsets;

  @JsonCreator
  private CommitOffsetsResponse(@Nullable List<Offset> offsets) {
    this.offsets = offsets;
  }

  @JsonValue
  @Nullable
  public List<Offset> getOffsets() {
    return offsets;
  }

  public static CommitOffsetsResponse fromOffsets(List<TopicPartitionOffset> offsets) {
    return new CommitOffsetsResponse(
        offsets.stream().map(Offset::fromTopicPartitionOffset).collect(Collectors.toList()));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    CommitOffsetsResponse that = (CommitOffsetsResponse) o;
    return Objects.equals(offsets, that.offsets);
  }

  @Override
  public int hashCode() {
    return Objects.hash(offsets);
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", CommitOffsetsResponse.class.getSimpleName() + "[", "]")
        .add("offsets=" + offsets)
        .toString();
  }

  public static final class Offset {

    @NotEmpty
    @Nullable
    private String topic;

    @PositiveOrZero
    @Nullable
    private Integer partition;

    @PositiveOrZero
    @Nullable
    private Long consumed;

    @PositiveOrZero
    @Nullable
    private Long committed;

    @JsonCreator
    private Offset(
        @JsonProperty("topic") @Nullable String topic,
        @JsonProperty("partition") @Nullable Integer partition,
        @JsonProperty("consumed") @Nullable Long consumed,
        @JsonProperty("committed") @Nullable Long committed
    ) {
      this.topic = topic;
      this.partition = partition;
      this.consumed = consumed;
      this.committed = committed;
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
    public Long getConsumed() {
      return consumed;
    }

    @JsonProperty
    @Nullable
    public Long getCommitted() {
      return committed;
    }

    public static Offset fromTopicPartitionOffset(TopicPartitionOffset offset) {
      return new Offset(
          offset.getTopic(), offset.getPartition(), offset.getConsumed(), offset.getCommitted());
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      Offset offset = (Offset) o;
      return Objects.equals(topic, offset.topic)
          && Objects.equals(partition, offset.partition)
          && Objects.equals(consumed, offset.consumed)
          && Objects.equals(committed, offset.committed);
    }

    @Override
    public int hashCode() {
      return Objects.hash(topic, partition, consumed, committed);
    }

    @Override
    public String toString() {
      return new StringJoiner(", ", Offset.class.getSimpleName() + "[", "]")
          .add("topic='" + topic + "'")
          .add("partition=" + partition)
          .add("consumed=" + consumed)
          .add("committed=" + committed)
          .toString();
    }
  }
}
