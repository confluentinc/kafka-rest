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

import static java.lang.Math.max;

import com.google.auto.value.AutoValue;
import java.util.Optional;

@AutoValue
public abstract class ConsumerLag {

  ConsumerLag() {
  }

  public abstract String getClusterId();

  public abstract String getConsumerGroupId();

  public abstract String getConsumerId();

  public abstract Optional<String> getInstanceId();

  public abstract String getClientId();

  public abstract String getTopicName();

  public abstract int getPartitionId();

  public abstract Long getCurrentOffset();

  public abstract Long getLogEndOffset();

  public final Long getLag() {
    return max(0, getLogEndOffset() - getCurrentOffset());
  }

  public static Builder builder() {
    return new AutoValue_ConsumerLag.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {

    Builder() {
    }

    public abstract Builder setClusterId(String clusterId);

    public abstract Builder setConsumerGroupId(String consumerGroupId);

    public abstract Builder setConsumerId(String consumerId);

    public abstract Builder setInstanceId(Optional<String> instanceId);

    public abstract Builder setClientId(String clientId);

    public abstract Builder setTopicName(String topicName);

    public abstract Builder setPartitionId(int partitionId);

    public abstract Builder setCurrentOffset(Long currentOffset);

    public abstract Builder setLogEndOffset(Long logEndOffset);

    public abstract ConsumerLag build();
  }
}
