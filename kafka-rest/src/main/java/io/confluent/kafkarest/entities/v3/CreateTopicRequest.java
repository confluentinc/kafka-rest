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
import java.util.List;
import java.util.Optional;
import javax.annotation.Nullable;

@AutoValue
public abstract class CreateTopicRequest {

  CreateTopicRequest() {
  }

  @JsonProperty("topic_name")
  public abstract String getTopicName();

  @JsonProperty("partitions_count")
  public abstract int getPartitionsCount();

  @JsonProperty("replication_factor")
  public abstract short getReplicationFactor();

  @JsonProperty("configs")
  public abstract ImmutableList<ConfigEntry> getConfigs();

  public static Builder builder() {
    return new AutoValue_CreateTopicRequest.Builder();
  }

  @JsonCreator
  static CreateTopicRequest fromJson(
      @JsonProperty("topic_name") String topicName,
      @JsonProperty("partitions_count") int partitionsCount,
      @JsonProperty("replication_factor") short replicationFactor,
      @JsonProperty("configs") @Nullable List<ConfigEntry> configs
  ) {
    return builder()
        .setTopicName(topicName)
        .setPartitionsCount(partitionsCount)
        .setReplicationFactor(replicationFactor)
        .setConfigs(configs != null ? configs : ImmutableList.of())
        .build();
  }

  @AutoValue.Builder
  public abstract static class Builder {

    Builder() {
    }

    public abstract Builder setTopicName(String topicName);

    public abstract Builder setPartitionsCount(int partitionsCount);

    public abstract Builder setReplicationFactor(short replicationFactor);

    public abstract Builder setConfigs(List<ConfigEntry> configs);

    public abstract CreateTopicRequest build();
  }

  @AutoValue
  public abstract static class ConfigEntry {

    ConfigEntry() {
    }

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
