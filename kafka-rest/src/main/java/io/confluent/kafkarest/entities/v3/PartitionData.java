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

package io.confluent.kafkarest.entities.v3;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.auto.value.AutoValue;
import io.confluent.kafkarest.entities.Partition;
import java.util.Optional;
import javax.annotation.Nullable;

@AutoValue
public abstract class PartitionData extends Resource {

  PartitionData() {
  }

  @JsonProperty("cluster_id")
  public abstract String getClusterId();

  @JsonProperty("topic_name")
  public abstract String getTopicName();

  @JsonProperty("partition_id")
  public abstract int getPartitionId();

  @JsonProperty("leader")
  public abstract Optional<Relationship> getLeader();

  @JsonProperty("replicas")
  public abstract Relationship getReplicas();

  @JsonProperty("reassignment")
  public abstract Relationship getReassignment();

  public static Builder builder() {
    return new AutoValue_PartitionData.Builder().setKind("KafkaPartition");
  }

  public static Builder fromPartition(Partition partition) {
    return builder()
        .setClusterId(partition.getClusterId())
        .setTopicName(partition.getTopicName())
        .setPartitionId(partition.getPartitionId());
  }

  @JsonCreator
  static PartitionData fromJson(
      @JsonProperty("kind") String kind,
      @JsonProperty("metadata") Metadata metadata,
      @JsonProperty("cluster_id") String clusterId,
      @JsonProperty("topic_name") String topicName,
      @JsonProperty("partition_id") int partitionId,
      @JsonProperty("leader") @Nullable Relationship leader,
      @JsonProperty("replicas") Relationship replicas,
      @JsonProperty("reassignment") Relationship reassignment
  ) {
    return builder()
        .setKind(kind)
        .setMetadata(metadata)
        .setClusterId(clusterId)
        .setTopicName(topicName)
        .setPartitionId(partitionId)
        .setLeader(leader)
        .setReplicas(replicas)
        .setReassignment(reassignment)
        .build();
  }

  @AutoValue.Builder
  public abstract static class Builder extends Resource.Builder<Builder> {

    Builder() {
    }

    public abstract Builder setClusterId(String clusterId);

    public abstract Builder setTopicName(String topicName);

    public abstract Builder setPartitionId(int partitionId);

    public abstract Builder setLeader(@Nullable Relationship leader);

    public abstract Builder setReplicas(Relationship replicas);

    public abstract Builder setReassignment(Relationship reassignment);

    public abstract PartitionData build();
  }
}
