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

@AutoValue
public abstract class ConsumerGroupLagSummary {

  ConsumerGroupLagSummary() {
  }

  public abstract String getClusterId();

  public abstract String getConsumerGroupId();

  public abstract String getMaxLagClientId();

  public abstract String getMaxLagConsumerId();

  public abstract Optional<String> getMaxLagInstanceId();

  public abstract String getMaxLagTopicName();

  public abstract Integer getMaxLagPartitionId();

  public abstract Long getMaxLag();

  public abstract Long getTotalLag();

  public static Builder builder() {
    return new AutoValue_ConsumerGroupLagSummary.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {

    private long maxLag = -1;
    private long totalLag = 0;

    Builder() {
    }

    public final void addOffset(
        String topicName,
        String consumerId,
        Optional<String> instanceId,
        String clientId,
        int partitionId,
        long currentOffset,
        long endOffset
    ) {
      // We don't report negative lag
      long lag = Math.max(0, endOffset - currentOffset);
      if (maxLag < lag) {
        maxLag = lag;
        setMaxLagConsumerId(consumerId);
        setMaxLagInstanceId(instanceId);
        setMaxLagClientId(clientId);
        setMaxLagTopicName(topicName);
        setMaxLagPartitionId(partitionId);
        setMaxLag(maxLag);
      }
      totalLag += lag;
      setTotalLag(totalLag);
    }

    public abstract Builder setClusterId(String clusterId);

    public abstract Builder setConsumerGroupId(String consumerGroupId);

    public abstract Builder setMaxLagClientId(String maxLagClientId);

    public abstract Builder setMaxLagConsumerId(String maxLagConsumerId);

    public abstract Builder setMaxLagInstanceId(Optional<String> maxLagInstanceId);

    public abstract Builder setMaxLagTopicName(String maxLagTopicName);

    public abstract Builder setMaxLagPartitionId(Integer maxLagPartitionId);

    public abstract Builder setMaxLag(Long maxLag);

    public abstract Builder setTotalLag(Long totalLag);

    public abstract ConsumerGroupLagSummary build();
  }
}
