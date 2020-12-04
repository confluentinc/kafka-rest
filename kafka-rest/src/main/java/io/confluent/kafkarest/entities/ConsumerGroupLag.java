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

import static java.lang.Math.toIntExact;

import com.google.auto.value.AutoValue;
import io.confluent.kafkarest.resources.ConsumerGroupOffsets;
import java.util.Optional;
import javax.annotation.Nullable;

@AutoValue
public abstract class ConsumerGroupLag {

  ConsumerGroupLag() {
  }

  public abstract Integer getMaxLag();

  public abstract Integer getTotalLag();

  public abstract String getClusterId();

  public abstract String getConsumerGroupId();

  public abstract String getMaxLagConsumerId();

  public abstract String getMaxLagClientId();

  public abstract Optional<String> getMaxLagInstanceId();

  public abstract String getMaxLagTopicName();

  public abstract Integer getMaxLagPartitionId();

  public static Builder builder() {
    return new AutoValue_ConsumerGroupLag.Builder();
  }

  // something similar to ConsumerGroup's fromConsumerGroupDescription(String clusterId, ConsumerGroupDescription description)
  // that can build and return a ConsumerGroupLag object
  // for instance, instead of return builder()...setPartitionAssignor(description.partitionAssignor())
  // we need to pull information like maxLag and totalLag from a ConsumerGroupLagDescription object to set on our builder
  public static ConsumerGroupLag fromConsumerGroupOffsets(
      String clusterId, ConsumerGroupOffsets cgo) {
    return builder()
        .setClusterId(clusterId)
        .setConsumerGroupId(cgo.getConsumerGroupId())
        .setMaxLag(42)
        .setTotalLag(toIntExact(cgo.getTotalLag()))
        .setMaxLagConsumerId("todo")
        .setMaxLagClientId("todo")
        .setMaxLagInstanceId("todo")
        .setMaxLagTopicName("todo")
        .build();
  }

  @AutoValue.Builder
  public abstract static class Builder {

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

//    public abstract Builder setMaxLagPartition(Partition maxLagPartition);
//
    public abstract ConsumerGroupLag build();
  }
}
