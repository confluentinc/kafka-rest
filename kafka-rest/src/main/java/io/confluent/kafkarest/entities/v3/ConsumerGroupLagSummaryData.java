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
import io.confluent.kafkarest.entities.ConsumerGroupLagSummary;
import java.util.Optional;
import javax.annotation.Nullable;

@AutoValue
public abstract class ConsumerGroupLagSummaryData extends Resource {

  ConsumerGroupLagSummaryData() {}

  @JsonProperty("cluster_id")
  public abstract String getClusterId();

  @JsonProperty("consumer_group_id")
  public abstract String getConsumerGroupId();

  @JsonProperty("max_lag")
  public abstract Long getMaxLag();

  @JsonProperty("total_lag")
  public abstract Long getTotalLag();

  @JsonProperty("max_lag_consumer_id")
  public abstract String getMaxLagConsumerId();

  @JsonProperty("max_lag_consumer")
  public abstract Relationship getMaxLagConsumer();

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
    return new AutoValue_ConsumerGroupLagSummaryData.Builder()
        .setKind("KafkaConsumerGroupLagSummary");
  }

  public static Builder fromConsumerGroupLagSummary(
      ConsumerGroupLagSummary consumerGroupLagSummary) {
    return builder()
        .setClusterId(consumerGroupLagSummary.getClusterId())
        .setConsumerGroupId(consumerGroupLagSummary.getConsumerGroupId())
        .setMaxLag(consumerGroupLagSummary.getMaxLag())
        .setTotalLag(consumerGroupLagSummary.getTotalLag())
        .setMaxLagConsumerId(consumerGroupLagSummary.getMaxLagConsumerId())
        .setMaxLagClientId(consumerGroupLagSummary.getMaxLagClientId())
        .setMaxLagInstanceId(consumerGroupLagSummary.getMaxLagInstanceId().orElse(null))
        .setMaxLagTopicName(consumerGroupLagSummary.getMaxLagTopicName())
        .setMaxLagPartitionId(consumerGroupLagSummary.getMaxLagPartitionId());
  }

  // CHECKSTYLE:OFF:ParameterNumber
  @JsonCreator
  static ConsumerGroupLagSummaryData fromJson(
      @JsonProperty("kind") String kind,
      @JsonProperty("metadata") Metadata metadata,
      @JsonProperty("cluster_id") String clusterId,
      @JsonProperty("consumer_group_id") String consumerGroupId,
      @JsonProperty("max_lag") Long maxLag,
      @JsonProperty("total_lag") Long totalLag,
      @JsonProperty("max_lag_consumer_id") String maxLagConsumerId,
      @JsonProperty("max_lag_consumer") Relationship maxLagConsumer,
      @JsonProperty("max_lag_client_id") String maxLagClientId,
      @JsonProperty("max_lag_instance_id") @Nullable String maxLagInstanceId,
      @JsonProperty("max_lag_topic_name") String maxLagTopicName,
      @JsonProperty("max_lag_partition_id") Integer maxLagPartitionId,
      @JsonProperty("max_lag_partition") Relationship maxLagPartition) {
    return builder()
        .setKind(kind)
        .setMetadata(metadata)
        .setClusterId(clusterId)
        .setConsumerGroupId(consumerGroupId)
        .setMaxLag(maxLag)
        .setTotalLag(totalLag)
        .setMaxLagConsumerId(maxLagConsumerId)
        .setMaxLagConsumer(maxLagConsumer)
        .setMaxLagClientId(maxLagClientId)
        .setMaxLagInstanceId(maxLagInstanceId)
        .setMaxLagTopicName(maxLagTopicName)
        .setMaxLagPartitionId(maxLagPartitionId)
        .setMaxLagPartition(maxLagPartition)
        .build();
  }
  // CHECKSTYLE:ON:ParameterNumber

  @AutoValue.Builder
  public abstract static class Builder extends Resource.Builder<Builder> {

    Builder() {}

    public abstract Builder setClusterId(String clusterId);

    public abstract Builder setConsumerGroupId(String consumerGroupId);

    public abstract Builder setMaxLag(Long maxLag);

    public abstract Builder setTotalLag(Long totalLag);

    public abstract Builder setMaxLagConsumerId(String maxLagConsumerId);

    public abstract Builder setMaxLagConsumer(Relationship maxLagConsumer);

    public abstract Builder setMaxLagClientId(String maxLagClientId);

    public abstract Builder setMaxLagInstanceId(@Nullable String maxLagInstanceId);

    public abstract Builder setMaxLagTopicName(String maxLagTopicName);

    public abstract Builder setMaxLagPartitionId(Integer maxLagPartitionId);

    public abstract Builder setMaxLagPartition(Relationship maxLagPartition);

    public abstract ConsumerGroupLagSummaryData build();
  }
}
