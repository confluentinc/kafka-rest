/*
 * Copyright 2025 Confluent Inc.
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

package io.confluent.kafkarest.entities.v3;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.auto.value.AutoValue;
import io.confluent.kafkarest.entities.Partition;

@AutoValue
public abstract class PartitionWithOffsetsData extends Resource {

  PartitionWithOffsetsData() {}

  @JsonProperty("cluster_id")
  public abstract String getClusterId();

  @JsonProperty("topic_name")
  public abstract String getTopicName();

  @JsonProperty("partition_id")
  public abstract int getPartitionId();

  @JsonProperty("earliest_offset")
  public abstract Long getEarliestOffset();

  @JsonProperty("latest_offset")
  public abstract Long getLatestOffset();

  public static Builder builder() {
    return new AutoValue_PartitionWithOffsetsData.Builder().setKind("KafkaPartitionWithOffsets");
  }

  public static Builder fromPartition(Partition partition) {
    return builder()
        .setClusterId(partition.getClusterId())
        .setTopicName(partition.getTopicName())
        .setPartitionId(partition.getPartitionId())
        .setEarliestOffset(partition.getEarliestOffset())
        .setLatestOffset(partition.getLatestOffset());
  }

  @JsonCreator
  static PartitionWithOffsetsData fromJson(
      @JsonProperty("kind") String kind,
      @JsonProperty("metadata") Metadata metadata,
      @JsonProperty("cluster_id") String clusterId,
      @JsonProperty("topic_name") String topicName,
      @JsonProperty("partition_id") int partitionId,
      @JsonProperty("earliest_offset") Long earliestOffset,
      @JsonProperty("latest_offset") Long latestOffset) {
    return builder()
        .setKind(kind)
        .setMetadata(metadata)
        .setClusterId(clusterId)
        .setTopicName(topicName)
        .setPartitionId(partitionId)
        .setEarliestOffset(earliestOffset)
        .setLatestOffset(latestOffset)
        .build();
  }

  @AutoValue.Builder
  public abstract static class Builder extends Resource.Builder<Builder> {

    Builder() {}

    public abstract Builder setClusterId(String clusterId);

    public abstract Builder setTopicName(String topicName);

    public abstract Builder setPartitionId(int partitionId);

    public abstract Builder setEarliestOffset(Long earliestOffset);

    public abstract Builder setLatestOffset(Long latestOffset);

    public abstract PartitionWithOffsetsData build();
  }
}
