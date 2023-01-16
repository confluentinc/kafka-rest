/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.kafkarest.entities.v2;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.confluent.kafkarest.entities.Topic;
import io.confluent.kafkarest.entities.TopicConfig;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.StringJoiner;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;

public final class GetTopicResponse {

  @NotEmpty
  @Nullable
  private final String name;

  @NotNull
  @Nullable
  private final Map<String, String> configs;

  @NotEmpty
  @Nullable
  private final List<GetPartitionResponse> partitions;

  @JsonCreator
  private GetTopicResponse(
      @JsonProperty("name") @Nullable String name,
      @JsonProperty("configs") @Nullable Map<String, String> configs,
      @JsonProperty("partitions") @Nullable List<GetPartitionResponse> partitions) {
    this.name = name;
    this.configs = configs;
    this.partitions = partitions;
  }

  @JsonProperty
  @Nullable
  public String getName() {
    return name;
  }

  @JsonProperty
  @Nullable
  public Map<String, String> getConfigs() {
    return configs;
  }

  @JsonProperty
  @Nullable
  public List<GetPartitionResponse> getPartitions() {
    return partitions;
  }

  public static GetTopicResponse fromTopic(Topic topic, List<TopicConfig> configs) {
    HashMap<String, String> configsMap = new HashMap<>();
    for (TopicConfig config : configs) {
      configsMap.put(config.getName(), config.getValue());
    }
    return new GetTopicResponse(
        topic.getName(),
        configsMap,
        topic.getPartitions()
            .stream()
            .map(GetPartitionResponse::fromPartition)
            .collect(Collectors.toList()));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    GetTopicResponse that = (GetTopicResponse) o;
    return Objects.equals(name, that.name)
        && Objects.equals(configs, that.configs)
        && Objects.equals(partitions, that.partitions);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, configs, partitions);
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", GetTopicResponse.class.getSimpleName() + "[", "]")
        .add("name='" + name + "'")
        .add("configs=" + configs)
        .add("partitions=" + partitions)
        .toString();
  }
}
