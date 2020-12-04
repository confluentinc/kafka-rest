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
import io.confluent.kafkarest.entities.ConsumerGroupLag;
import java.util.Optional;
import javax.annotation.Nullable;

@AutoValue
public abstract class ConsumerGroupLagData extends Resource {

  ConsumerGroupLagData() {
  }
  @JsonProperty("cluster_id")
  public abstract String getClusterId();

  @JsonProperty("consumer_group_id")
  public abstract String getConsumerGroupId();

  @JsonProperty("max_lag")
  public abstract Integer getMaxLag();

  @JsonProperty("total_lag")
  public abstract Integer getTotalLag();

  @JsonProperty("max_lag_consumer_id")
  public abstract String getMaxLagConsumerId();

  @JsonProperty("max_lag_client_id")
  public abstract String getMaxLagClientId();

  @JsonProperty("max_lag_instance_id")
  public abstract Optional<String> getMaxLagInstanceId();

  @JsonProperty("max_lag_topic_name")
  public abstract String getMaxLagTopicName();

  @JsonProperty("max_lag_partition_id")
  public abstract Integer getMaxLagPartitionId();

  @JsonProperty("max_lag_partition")
  public abstract Relationship getMaxLagPartition();

  public static Builder builder() {
    return new AutoValue_ConsumerGroupLagData.Builder().setKind("KafkaConsumerGroupLag");
  }

  // what ConsumerGroupLag get methods are appropriate?
  public static Builder fromConsumerGroupLag(ConsumerGroupLag consumerGroupLag) {
    return builder()
        .setClusterId(consumerGroupLag.getClusterId())
        .setConsumerGroupId(consumerGroupLag.getConsumerGroupId())
        .setMaxLag(consumerGroupLag.getMaxLag())
        .setTotalLag(consumerGroupLag.getTotalLag())
        .setMaxLagConsumerId(consumerGroupLag.getMaxLagConsumerId())
        .setMaxLagClientId(consumerGroupLag.getMaxLagClientId())
        .setMaxLagInstanceId(consumerGroupLag.getMaxLagInstanceId().orElse(null))
        .setMaxLagTopicName(consumerGroupLag.getMaxLagTopicName())
        .setMaxLagPartitionId(consumerGroupLag.getMaxLagPartitionId());
  }

  @JsonCreator
  static ConsumerGroupLagData fromJson(
      @JsonProperty("kind") String kind,
      @JsonProperty("metadata") Metadata metadata,
      @JsonProperty("cluster_id") String clusterId,
      @JsonProperty("consumer_group_id") String consumerGroupId,
      @JsonProperty("max_lag") Integer maxLag,
      @JsonProperty("total_lag") Integer totalLag,
      @JsonProperty("max_lag_consumer_id") String maxLagConsumerId,
      @JsonProperty("max_lag_client_id") String maxLagClientId,
      @JsonProperty("max_lag_instance_id") @Nullable String maxLagInstanceId,
      @JsonProperty("max_lag_topic_name") String maxLagTopicName,
      @JsonProperty("max_lag_partition_id") Integer maxLagPartitionId,
      @JsonProperty("max_lag_partition") Relationship maxLagPartition
  ) {
    return builder()
        .setKind(kind)
        .setMetadata(metadata)
        .setClusterId(clusterId)
        .setConsumerGroupId(consumerGroupId)
        .setMaxLag(maxLag)
        .setTotalLag(totalLag)
        .setMaxLagConsumerId(maxLagConsumerId)
        .setMaxLagClientId(maxLagClientId)
        .setMaxLagInstanceId(maxLagInstanceId)
        .setMaxLagTopicName(maxLagTopicName)
        .setMaxLagPartitionId(maxLagPartitionId)
        .setMaxLagPartition(maxLagPartition)
        .build();
  }

  @AutoValue.Builder
  public abstract static class Builder extends Resource.Builder<Builder> {

    Builder() {
    }

    public abstract Builder setClusterId(String clusterId);

    public abstract Builder setConsumerGroupId(String consumerGroupId);

    public abstract Builder setMaxLag(Integer maxLag);

    public abstract Builder setTotalLag(Integer totalLag);

    public abstract Builder setMaxLagConsumerId(String maxLagConsumerId);

    public abstract Builder setMaxLagClientId(String maxLagClientId);

    public abstract Builder setMaxLagInstanceId(@Nullable String maxLagInstanceId);

    public abstract Builder setMaxLagTopicName(String maxLagTopicName);

    public abstract Builder setMaxLagPartitionId(Integer maxLagPartitionId);

    public abstract Builder setMaxLagPartition(Relationship maxLagPartition);

    public abstract ConsumerGroupLagData build();
  }
}
