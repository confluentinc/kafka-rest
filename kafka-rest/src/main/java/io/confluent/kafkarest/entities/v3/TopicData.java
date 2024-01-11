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
import com.google.common.collect.ImmutableSet;
import io.confluent.kafkarest.entities.Acl;
import io.confluent.kafkarest.entities.Topic;
import java.util.Set;

// CHECKSTYLE:OFF:ParameterNumber
@AutoValue
public abstract class TopicData extends Resource {

  TopicData() {}

  @JsonProperty("cluster_id")
  public abstract String getClusterId();

  @JsonProperty("topic_name")
  public abstract String getTopicName();

  @JsonProperty("is_internal")
  public abstract boolean isInternal();

  @JsonProperty("replication_factor")
  public abstract int getReplicationFactor();

  @JsonProperty("partitions_count")
  public abstract int getPartitionsCount();

  @JsonProperty("partitions")
  public abstract Relationship getPartitions();

  @JsonProperty("configs")
  public abstract Relationship getConfigs();

  @JsonProperty("partition_reassignments")
  public abstract Relationship getPartitionReassignments();

  @JsonProperty("authorized_operations")
  public abstract ImmutableSet<Acl.Operation> getAuthorizedOperations();

  public static Builder builder() {
    return new AutoValue_TopicData.Builder().setKind("KafkaTopic");
  }

  public static Builder fromTopic(Topic topic) {
    return builder()
        .setClusterId(topic.getClusterId())
        .setTopicName(topic.getName())
        .setInternal(topic.isInternal())
        .setReplicationFactor(topic.getReplicationFactor())
        .setPartitionsCount(topic.getPartitions().size())
        .setAuthorizedOperations(topic.getAuthorizedOperations());
  }

  @JsonCreator
  static TopicData fromJson(
      @JsonProperty("kind") String kind,
      @JsonProperty("metadata") Metadata metadata,
      @JsonProperty("cluster_id") String clusterId,
      @JsonProperty("topic_name") String topicName,
      @JsonProperty("is_internal") boolean isInternal,
      @JsonProperty("replication_factor") int replicationFactor,
      @JsonProperty("partitions_count") int partitionsCount,
      @JsonProperty("partitions") Relationship partitions,
      @JsonProperty("authorized_operations") ImmutableSet<Acl.Operation> authorizedOperations,
      @JsonProperty("configs") Relationship configs,
      @JsonProperty("partition_reassignments") Relationship partitionReassignments) {
    return builder()
        .setKind(kind)
        .setMetadata(metadata)
        .setClusterId(clusterId)
        .setTopicName(topicName)
        .setInternal(isInternal)
        .setReplicationFactor(replicationFactor)
        .setPartitionsCount(partitionsCount)
        .setPartitions(partitions)
        .setConfigs(configs)
        .setPartitionReassignments(partitionReassignments)
        .setAuthorizedOperations(authorizedOperations)
        .build();
  }

  @AutoValue.Builder
  public abstract static class Builder extends Resource.Builder<Builder> {

    Builder() {}

    public abstract Builder setClusterId(String clusterId);

    public abstract Builder setTopicName(String topicName);

    public abstract Builder setInternal(boolean isInternal);

    public abstract Builder setReplicationFactor(int replicationFactor);

    public abstract Builder setPartitionsCount(int partitionsCount);

    public abstract Builder setPartitions(Relationship partitions);

    public abstract Builder setConfigs(Relationship configs);

    public abstract Builder setPartitionReassignments(Relationship partitionReassignments);

    public abstract Builder setAuthorizedOperations(Set<Acl.Operation> authorizedOperations);

    public abstract TopicData build();
  }
}
// CHECKSTYLE:ON:ParameterNumber
