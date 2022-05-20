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
import com.google.common.collect.ImmutableList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nullable;

@AutoValue
public abstract class CreateTopicRequest {

  CreateTopicRequest() {}

  @JsonProperty("topic_name")
  public abstract String getTopicName();

  @JsonProperty("partitions_count")
  public abstract Optional<Integer> getPartitionsCount();

  @JsonProperty("replication_factor")
  public abstract Optional<Short> getReplicationFactor();

  @JsonProperty("replicas_assignments")
  public abstract Map<Integer, List<Integer>> getReplicasAssignments();

  @JsonProperty("configs")
  public abstract ImmutableList<ConfigEntry> getConfigs();

  public static Builder builder() {
    return new AutoValue_CreateTopicRequest.Builder()
        .setReplicasAssignments(Collections.emptyMap());
  }

  @JsonCreator
  static CreateTopicRequest fromJson(
      @JsonProperty("topic_name") String topicName,
      @JsonProperty("partitions_count") @Nullable Integer partitionsCount,
      @JsonProperty("replication_factor") @Nullable Short replicationFactor,
      @JsonProperty("replicas_assignments") @Nullable
          Map<Integer, List<Integer>> replicasAssignments,
      @JsonProperty("configs") @Nullable List<ConfigEntry> configs) {
    return builder()
        .setTopicName(topicName)
        .setPartitionsCount(partitionsCount)
        .setReplicationFactor(replicationFactor)
        .setReplicasAssignments(
            replicasAssignments != null ? replicasAssignments : Collections.emptyMap())
        .setConfigs(configs != null ? configs : ImmutableList.of())
        .build();
  }

  @AutoValue.Builder
  public abstract static class Builder {

    Builder() {}

    public abstract Builder setTopicName(String topicName);

    public abstract Builder setPartitionsCount(@Nullable Integer partitionsCount);

    public abstract Builder setReplicationFactor(@Nullable Short replicationFactor);

    public abstract Builder setReplicasAssignments(Map<Integer, List<Integer>> replicasAssignments);

    public abstract Builder setConfigs(List<ConfigEntry> configs);

    public abstract CreateTopicRequest build();
  }

  @AutoValue
  public abstract static class ConfigEntry {

    ConfigEntry() {}

    @JsonProperty("name")
    public abstract String getName();

    @JsonProperty("value")
    public abstract Optional<String> getValue();

    public static ConfigEntry create(String name, @Nullable String value) {
      return new AutoValue_CreateTopicRequest_ConfigEntry(name, Optional.ofNullable(value));
    }

    @JsonCreator
    static ConfigEntry fromJson(
        @JsonProperty("name") String name, @JsonProperty("value") @Nullable String value) {
      return create(name, value);
    }
  }
}
