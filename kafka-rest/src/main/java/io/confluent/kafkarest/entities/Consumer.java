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

import static java.util.Collections.emptyList;

import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.kafka.clients.admin.MemberDescription;

@AutoValue
public abstract class Consumer {

  Consumer() {
  }

  public abstract String getClusterId();

  public abstract String getConsumerGroupId();

  public abstract String getConsumerId();

  public abstract Optional<String> getInstanceId();

  public abstract String getClientId();

  public abstract String getHost();

  public abstract ImmutableList<Partition> getAssignedPartitions();

  public static Builder builder() {
    return new AutoValue_Consumer.Builder();
  }

  public static Consumer fromMemberDescription(
      String clusterId, String consumerGroupId, MemberDescription description) {
    return builder()
        .setClusterId(clusterId)
        .setConsumerGroupId(consumerGroupId)
        .setConsumerId(description.consumerId())
        .setInstanceId(description.groupInstanceId().orElse(null))
        .setClientId(description.clientId())
        .setHost(description.host())
        .setAssignedPartitions(
            description.assignment()
                .topicPartitions()
                .stream()
                .map(
                    partition ->
                        Partition.create(
                            clusterId,
                            partition.topic(),
                            partition.partition(),
                            /* replicas= */ emptyList()))
                .collect(Collectors.toList()))
        .build();
  }

  @AutoValue.Builder
  public abstract static class Builder {

    Builder() {
    }

    public abstract Builder setClusterId(String clusterId);

    public abstract Builder setConsumerGroupId(String consumerGroupId);

    public abstract Builder setConsumerId(String consumerId);

    public abstract Builder setInstanceId(@Nullable String instanceId);

    public abstract Builder setClientId(String clientId);

    public abstract Builder setHost(String host);

    public abstract Builder setAssignedPartitions(List<Partition> assignedPartitions);

    public abstract Consumer build();
  }
}
