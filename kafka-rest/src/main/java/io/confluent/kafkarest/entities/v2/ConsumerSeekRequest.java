/*
 * Copyright 2020 Confluent Inc.
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
import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableList;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nullable;

@AutoValue
public abstract class ConsumerSeekRequest {

  ConsumerSeekRequest() {}

  @JsonProperty("offsets")
  public abstract ImmutableList<PartitionOffset> getOffsets();

  @JsonProperty("timestamps")
  public abstract ImmutableList<PartitionTimestamp> getTimestamps();

  @JsonCreator
  static ConsumerSeekRequest fromJson(
      @JsonProperty("offsets") @Nullable List<PartitionOffset> offsets,
      @JsonProperty("timestamps") @Nullable List<PartitionTimestamp> timestamps) {
    return new AutoValue_ConsumerSeekRequest(
        offsets != null ? ImmutableList.copyOf(offsets) : ImmutableList.of(),
        timestamps != null ? ImmutableList.copyOf(timestamps) : ImmutableList.of());
  }

  @AutoValue
  public abstract static class PartitionOffset {

    PartitionOffset() {}

    @JsonProperty("topic")
    public abstract String getTopic();

    @JsonProperty("partition")
    public abstract int getPartition();

    @JsonProperty("offset")
    public abstract long getOffset();

    @JsonProperty("metadata")
    public abstract Optional<String> getMetadata();

    @JsonCreator
    static PartitionOffset fromJson(
        @JsonProperty("topic") String topic,
        @JsonProperty("partition") int partition,
        @JsonProperty("offset") long offset,
        @JsonProperty("metadata") @Nullable String metadata) {
      return build()
          .setTopic(topic)
          .setPartition(partition)
          .setOffset(offset)
          .setMetadata(metadata)
          .build();
    }

    public static Builder build() {
      return new AutoValue_ConsumerSeekRequest_PartitionOffset.Builder();
    }

    @AutoValue.Builder
    public abstract static class Builder {

      Builder() {}

      public abstract Builder setTopic(String topic);

      public abstract Builder setPartition(int partition);

      public abstract Builder setOffset(long offset);

      public abstract Builder setMetadata(@Nullable String metadata);

      public abstract PartitionOffset build();
    }
  }

  @AutoValue
  public abstract static class PartitionTimestamp {

    PartitionTimestamp() {}

    @JsonProperty("topic")
    public abstract String getTopic();

    @JsonProperty("partition")
    public abstract int getPartition();

    @JsonProperty("timestamp")
    public abstract Instant getTimestamp();

    @JsonProperty("metadata")
    public abstract Optional<String> getMetadata();

    @JsonCreator
    static PartitionTimestamp fromJson(
        @JsonProperty("topic") String topic,
        @JsonProperty("partition") int partition,
        @JsonProperty("timestamp") Instant timestamp,
        @JsonProperty("metadata") @Nullable String metadata) {
      return build()
          .setTopic(topic)
          .setPartition(partition)
          .setTimestamp(timestamp)
          .setMetadata(metadata)
          .build();
    }

    public static Builder build() {
      return new AutoValue_ConsumerSeekRequest_PartitionTimestamp.Builder();
    }

    @AutoValue.Builder
    public abstract static class Builder {

      Builder() {}

      public abstract Builder setTopic(String topic);

      public abstract Builder setPartition(int partition);

      public abstract Builder setTimestamp(Instant timestamp);

      public abstract Builder setMetadata(@Nullable String metadata);

      public abstract PartitionTimestamp build();
    }
  }
}
