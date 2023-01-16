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
import io.confluent.kafkarest.entities.Consumer;
import java.util.Optional;
import javax.annotation.Nullable;

@AutoValue
public abstract class ConsumerData extends Resource {

  ConsumerData() {
  }

  @JsonProperty("cluster_id")
  public abstract String getClusterId();

  @JsonProperty("consumer_group_id")
  public abstract String getConsumerGroupId();

  @JsonProperty("consumer_id")
  public abstract String getConsumerId();

  @JsonProperty("instance_id")
  public abstract Optional<String> getInstanceId();

  @JsonProperty("client_id")
  public abstract String getClientId();

  @JsonProperty("assignments")
  public abstract Relationship getAssignments();

  public static Builder builder() {
    return new AutoValue_ConsumerData.Builder().setKind("KafkaConsumer");
  }

  public static Builder fromConsumer(Consumer consumer) {
    return builder()
        .setClusterId(consumer.getClusterId())
        .setConsumerGroupId(consumer.getConsumerGroupId())
        .setConsumerId(consumer.getConsumerId())
        .setInstanceId(consumer.getInstanceId().orElse(null))
        .setClientId(consumer.getClientId());
  }

  @JsonCreator
  static ConsumerData fromJson(
      @JsonProperty("kind") String kind,
      @JsonProperty("metadata") Metadata metadata,
      @JsonProperty("cluster_id") String clusterId,
      @JsonProperty("consumer_group_id") String consumerGroupId,
      @JsonProperty("consumer_id") String consumerId,
      @JsonProperty("instance_id") @Nullable String instanceId,
      @JsonProperty("client_id") String clientId,
      @JsonProperty("assignments") Relationship assignments
  ) {
    return builder()
        .setKind(kind)
        .setMetadata(metadata)
        .setClusterId(clusterId)
        .setConsumerGroupId(consumerGroupId)
        .setConsumerId(consumerId)
        .setInstanceId(instanceId)
        .setClientId(clientId)
        .setAssignments(assignments)
        .build();
  }

  @AutoValue.Builder
  public abstract static class Builder extends Resource.Builder<Builder> {

    Builder() {
    }

    public abstract Builder setClusterId(String clusterId);

    public abstract Builder setConsumerGroupId(String consumerGroupId);

    public abstract Builder setConsumerId(String consumerId);

    public abstract Builder setInstanceId(@Nullable String instanceId);

    public abstract Builder setClientId(String clientId);

    public abstract Builder setAssignments(Relationship assignedPartitions);

    public abstract ConsumerData build();
  }
}
