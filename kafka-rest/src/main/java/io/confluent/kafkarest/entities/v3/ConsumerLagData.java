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
import io.confluent.kafkarest.entities.ConsumerLag;
import java.util.Optional;
import javax.annotation.Nullable;

@AutoValue
public abstract class ConsumerLagData extends Resource {

  ConsumerLagData() {}

  @JsonProperty("cluster_id")
  public abstract String getClusterId();

  @JsonProperty("consumer_group_id")
  public abstract String getConsumerGroupId();

  @JsonProperty("topic_name")
  public abstract String getTopicName();

  @JsonProperty("partition_id")
  public abstract int getPartitionId();

  @JsonProperty("consumer_id")
  public abstract String getConsumerId();

  @JsonProperty("instance_id")
  public abstract Optional<String> getInstanceId();

  @JsonProperty("client_id")
  public abstract String getClientId();

  @JsonProperty("current_offset")
  public abstract Long getCurrentOffset();

  @JsonProperty("log_end_offset")
  public abstract Long getLogEndOffset();

  @JsonProperty("lag")
  public abstract Long getLag();

  public static Builder builder() {
    return new AutoValue_ConsumerLagData.Builder().setKind("KafkaConsumerLag");
  }

  public static Builder fromConsumerLag(ConsumerLag consumerLag) {
    return builder()
        .setClusterId(consumerLag.getClusterId())
        .setConsumerGroupId(consumerLag.getConsumerGroupId())
        .setTopicName(consumerLag.getTopicName())
        .setPartitionId(consumerLag.getPartitionId())
        .setConsumerId(consumerLag.getConsumerId())
        .setInstanceId(consumerLag.getInstanceId().orElse(null))
        .setClientId(consumerLag.getClientId())
        .setCurrentOffset(consumerLag.getCurrentOffset())
        .setLogEndOffset(consumerLag.getLogEndOffset())
        .setLag(consumerLag.getLag());
  }

  // CHECKSTYLE:OFF:ParameterNumber
  @JsonCreator
  static ConsumerLagData fromJson(
      @JsonProperty("kind") String kind,
      @JsonProperty("metadata") Metadata metadata,
      @JsonProperty("cluster_id") String clusterId,
      @JsonProperty("consumer_group_id") String consumerGroupId,
      @JsonProperty("topic_name") String topicName,
      @JsonProperty("partition_id") int partitionId,
      @JsonProperty("consumer_id") String consumerId,
      @JsonProperty("instance_id") @Nullable String instanceId,
      @JsonProperty("client_id") String clientId,
      @JsonProperty("current_offset") Long currentOffset,
      @JsonProperty("log_end_offset") Long logEndOffset,
      @JsonProperty("lag") Long lag) {
    return builder()
        .setKind(kind)
        .setMetadata(metadata)
        .setClusterId(clusterId)
        .setConsumerGroupId(consumerGroupId)
        .setTopicName(topicName)
        .setPartitionId(partitionId)
        .setConsumerId(consumerId)
        .setInstanceId(instanceId)
        .setClientId(clientId)
        .setCurrentOffset(currentOffset)
        .setLogEndOffset(logEndOffset)
        .setLag(lag)
        .build();
  }
  // CHECKSTYLE:ON:ParameterNumber

  @AutoValue.Builder
  public abstract static class Builder extends Resource.Builder<Builder> {

    Builder() {}

    public abstract Builder setClusterId(String clusterId);

    public abstract Builder setConsumerGroupId(String consumerGroupId);

    public abstract Builder setTopicName(String topicName);

    public abstract Builder setPartitionId(int partitionId);

    public abstract Builder setConsumerId(String consumerId);

    public abstract Builder setInstanceId(@Nullable String instanceId);

    public abstract Builder setClientId(String clientId);

    public abstract Builder setCurrentOffset(Long currentOffset);

    public abstract Builder setLogEndOffset(Long logEndOffset);

    public abstract Builder setLag(Long lag);

    public abstract ConsumerLagData build();
  }
}
