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
import io.confluent.kafkarest.entities.ConsumerAssignment;

@AutoValue
public abstract class ConsumerAssignmentData extends Resource {

  ConsumerAssignmentData() {
  }

  @JsonProperty("cluster_id")
  public abstract String getClusterId();

  @JsonProperty("consumer_group_id")
  public abstract String getConsumerGroupId();

  @JsonProperty("consumer_id")
  public abstract String getConsumerId();

  @JsonProperty("topic_name")
  public abstract String getTopicName();

  @JsonProperty("partition_id")
  public abstract int getPartitionId();

  @JsonProperty("partition")
  public abstract Relationship getPartition();

  public static Builder builder() {
    return new AutoValue_ConsumerAssignmentData.Builder().setKind("KafkaConsumerAssignment");
  }

  public static Builder fromConsumerAssignment(ConsumerAssignment assignment) {
    return builder()
        .setClusterId(assignment.getClusterId())
        .setConsumerGroupId(assignment.getConsumerGroupId())
        .setConsumerId(assignment.getConsumerId())
        .setTopicName(assignment.getTopicName())
        .setPartitionId(assignment.getPartitionId());
  }

  @JsonCreator
  static ConsumerAssignmentData fromJson(
      @JsonProperty("kind") String kind,
      @JsonProperty("metadata") Metadata metadata,
      @JsonProperty("cluster_id") String clusterId,
      @JsonProperty("consumer_group_id") String consumerGroupId,
      @JsonProperty("consumer_id") String consumerId,
      @JsonProperty("topic_name") String topicName,
      @JsonProperty("partition_id") int partitionId,
      @JsonProperty("partition") Relationship partition
  ) {
    return builder()
        .setKind(kind)
        .setMetadata(metadata)
        .setClusterId(clusterId)
        .setConsumerGroupId(consumerGroupId)
        .setConsumerId(consumerId)
        .setTopicName(topicName)
        .setPartitionId(partitionId)
        .setPartition(partition)
        .build();
  }

  @AutoValue.Builder
  public abstract static class Builder extends Resource.Builder<Builder> {

    Builder() {
    }

    public abstract Builder setClusterId(String clusterId);

    public abstract Builder setConsumerGroupId(String consumerGroupId);

    public abstract Builder setConsumerId(String consumerId);

    public abstract Builder setTopicName(String topicName);

    public abstract Builder setPartitionId(int partitionId);

    public abstract Builder setPartition(Relationship partition);

    public abstract ConsumerAssignmentData build();
  }
}
