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
import com.google.common.collect.ImmutableList;
import io.confluent.kafkarest.entities.Reassignment;
import java.util.List;

@AutoValue
public abstract class ReassignmentData extends Resource {

  ReassignmentData() {
  }

  @JsonProperty("cluster_id")
  public abstract String getClusterId();

  @JsonProperty("topic_name")
  public abstract String getTopicName();

  @JsonProperty("partition_id")
  public abstract int getPartitionId();

  @JsonProperty("adding_replicas")
  public abstract ImmutableList<Integer> getAddingReplicas();

  @JsonProperty("removing_replicas")
  public abstract ImmutableList<Integer> getRemovingReplicas();

  @JsonProperty("replicas")
  public abstract Relationship getReplicas();

  public static Builder builder() {
    return new AutoValue_ReassignmentData.Builder().setKind("KafkaReassignment");
  }

  public static Builder fromReassignment(Reassignment reassignment) {
    return builder()
        .setClusterId(reassignment.getClusterId())
        .setTopicName(reassignment.getTopicName())
        .setPartitionId(reassignment.getPartitionId())
        .setAddingReplicas(reassignment.getAddingReplicas())
        .setRemovingReplicas(reassignment.getRemovingReplicas());
  }

  @JsonCreator
  static ReassignmentData fromJson(
      @JsonProperty("kind") String kind,
      @JsonProperty("metadata") Metadata metadata,
      @JsonProperty("cluster_id") String clusterId,
      @JsonProperty("topic_name") String topicName,
      @JsonProperty("partition_id") int partitionId,
      @JsonProperty("adding_replicas") List<Integer> addingReplicas,
      @JsonProperty("removing_replicas") List<Integer> removingReplicas,
      @JsonProperty("replicas") Relationship replicas
  ) {
    return builder()
        .setKind(kind)
        .setMetadata(metadata)
        .setClusterId(clusterId)
        .setTopicName(topicName)
        .setPartitionId(partitionId)
        .setAddingReplicas(addingReplicas)
        .setRemovingReplicas(removingReplicas)
        .setReplicas(replicas)
        .build();
  }

  @AutoValue.Builder
  public abstract static class Builder extends Resource.Builder<Builder> {

    Builder() {
    }

    public abstract Builder setClusterId(String clusterId);

    public abstract Builder setTopicName(String topicName);

    public abstract Builder setPartitionId(int partitionId);

    public abstract Builder setAddingReplicas(List<Integer> addingReplicas);

    public abstract Builder setRemovingReplicas(List<Integer> removingReplicas);

    public abstract Builder setReplicas(Relationship replicas);

    public abstract ReassignmentData build();
  }
}
