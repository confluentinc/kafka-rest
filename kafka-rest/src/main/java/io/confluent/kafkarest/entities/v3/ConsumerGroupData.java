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
import io.confluent.kafkarest.entities.ConsumerGroup;
import io.confluent.kafkarest.entities.ConsumerGroup.State;
import io.confluent.kafkarest.entities.ConsumerGroup.Type;

@AutoValue
public abstract class ConsumerGroupData extends Resource {

  ConsumerGroupData() {}

  @JsonProperty("cluster_id")
  public abstract String getClusterId();

  @JsonProperty("consumer_group_id")
  public abstract String getConsumerGroupId();

  @JsonProperty("is_simple")
  public abstract boolean isSimple();

  @JsonProperty("partition_assignor")
  public abstract String getPartitionAssignor();

  @JsonProperty("state")
  public abstract State getState();

  @JsonProperty("type")
  public abstract Type getType();

  @JsonProperty("is_mixed_consumer_group")
  public abstract boolean isMixedConsumerGroup();

  @JsonProperty("coordinator")
  public abstract Relationship getCoordinator();

  @JsonProperty("consumers")
  public abstract Relationship getConsumers();

  @JsonProperty("lag_summary")
  public abstract Relationship getLagSummary();

  public static Builder builder() {
    return new AutoValue_ConsumerGroupData.Builder().setKind("KafkaConsumerGroup");
  }

  public static Builder fromConsumerGroup(ConsumerGroup consumerGroup) {
    return builder()
        .setClusterId(consumerGroup.getClusterId())
        .setConsumerGroupId(consumerGroup.getConsumerGroupId())
        .setSimple(consumerGroup.isSimple())
        .setPartitionAssignor(consumerGroup.getPartitionAssignor())
        .setState(consumerGroup.getState())
        .setType(consumerGroup.getType())
        .setMixedConsumerGroup(consumerGroup.isMixedConsumerGroup());
  }

  // CHECKSTYLE:OFF:ParameterNumber
  @JsonCreator
  static ConsumerGroupData fromJson(
      @JsonProperty("kind") String kind,
      @JsonProperty("metadata") Metadata metadata,
      @JsonProperty("cluster_id") String clusterId,
      @JsonProperty("consumer_group_id") String consumerGroupId,
      @JsonProperty("is_simple") boolean isSimple,
      @JsonProperty("partition_assignor") String partitionAssignor,
      @JsonProperty("state") State state,
      @JsonProperty("type") Type type,
      @JsonProperty("is_mixed_consumer_group") boolean isMixedConsumerGroup,
      @JsonProperty("coordinator") Relationship coordinator,
      @JsonProperty("consumers") Relationship consumers,
      @JsonProperty("lag_summary") Relationship lagSummary) {
    return builder()
        .setKind(kind)
        .setMetadata(metadata)
        .setClusterId(clusterId)
        .setConsumerGroupId(consumerGroupId)
        .setSimple(isSimple)
        .setPartitionAssignor(partitionAssignor)
        .setState(state)
        .setType(type)
        .setMixedConsumerGroup(isMixedConsumerGroup)
        .setCoordinator(coordinator)
        .setConsumers(consumers)
        .setLagSummary(lagSummary)
        .build();
  }
  // CHECKSTYLE:ON:ParameterNumber

  @AutoValue.Builder
  public abstract static class Builder extends Resource.Builder<Builder> {

    Builder() {}

    public abstract Builder setClusterId(String clusterId);

    public abstract Builder setConsumerGroupId(String consumerGroupId);

    public abstract Builder setSimple(boolean isSimple);

    public abstract Builder setPartitionAssignor(String partitionAssignor);

    public abstract Builder setState(State state);

    public abstract Builder setType(Type type);

    public abstract Builder setMixedConsumerGroup(boolean isMixedConsumerGroup);

    public abstract Builder setCoordinator(Relationship coordinator);

    public abstract Builder setConsumers(Relationship consumers);

    public abstract Builder setLagSummary(Relationship lagSummary);

    public abstract ConsumerGroupData build();
  }
}
