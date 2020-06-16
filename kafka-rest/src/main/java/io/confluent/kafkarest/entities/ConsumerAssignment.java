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

@AutoValue
public abstract class ConsumerAssignment {

  ConsumerAssignment() {
  }

  public abstract String getClusterId();

  public abstract String getConsumerGroupId();

  public abstract String getConsumerId();

  public abstract String getTopicName();

  public abstract int getPartitionId();

  public static Builder builder() {
    return new AutoValue_ConsumerAssignment.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {

    Builder() {
    }

    public abstract Builder setClusterId(String clusterId);

    public abstract Builder setConsumerGroupId(String consumerGroupId);

    public abstract Builder setConsumerId(String consumerId);

    public abstract Builder setTopicName(String topicName);

    public abstract Builder setPartitionId(int partitionId);

    public abstract ConsumerAssignment build();
  }
}
