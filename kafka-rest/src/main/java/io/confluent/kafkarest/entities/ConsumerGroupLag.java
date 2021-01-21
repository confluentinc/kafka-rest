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

package io.confluent.kafkarest.entities;

import com.google.auto.value.AutoValue;
import java.util.Optional;
import javax.annotation.Nullable;

@AutoValue
public abstract class ConsumerGroupLag {

  ConsumerGroupLag() {
  }

  public abstract Long getMaxLag();

  public abstract Long getTotalLag();

  public abstract String getClusterId();

  public abstract String getConsumerGroupId();

  public abstract String getMaxLagClientId();

  public abstract String getMaxLagConsumerId();

  public abstract Optional<String> getMaxLagInstanceId();

  public abstract String getMaxLagTopicName();

  public abstract Integer getMaxLagPartitionId();

  public static Builder builder() {
    return new AutoValue_ConsumerGroupLag.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {

    private Long maxLag;
    private long totalLag = 0;

    Builder() {
    }

    public void addOffset(
        String topicName,
        String consumerId,
        String clientId,
        @Nullable String instanceId,
        int partitionId,
        long currentOffset,
        long endOffset
    ) {
      long lag = endOffset - currentOffset;
      if (maxLag == null || maxLag < lag) {
        maxLag = lag;
        setMaxLag(maxLag);
        setMaxLagClientId(clientId);
        setMaxLagConsumerId(consumerId);
        setMaxLagInstanceId(instanceId);
        setMaxLagTopicName(topicName);
        setMaxLagPartitionId(partitionId);
      }
      totalLag += lag;
      setTotalLag(totalLag);
    }

    public abstract Builder setClusterId(String clusterId);

    public abstract Builder setConsumerGroupId(String consumerGroupId);

    public abstract Builder setMaxLag(Long maxLag);

    public abstract Builder setTotalLag(Long totalLag);

    public abstract Builder setMaxLagClientId(String maxLagClientId);

    public abstract Builder setMaxLagConsumerId(String maxLagConsumerId);

    public abstract Builder setMaxLagInstanceId(@Nullable String maxLagInstanceId);

    public abstract Builder setMaxLagTopicName(String maxLagTopicName);

    public abstract Builder setMaxLagPartitionId(Integer maxLagPartitionId);

    public abstract ConsumerGroupLag build();
  }
}
