/*
 * Copyright 2026 Confluent Inc.
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

import static java.util.Collections.unmodifiableList;
import static java.util.Collections.unmodifiableMap;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableList;
import io.confluent.kafkarest.entities.AlterConfigCommand;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@AutoValue
public abstract class AlterMultipleTopicsConfigsBatchRequestData {

  AlterMultipleTopicsConfigsBatchRequestData() {}

  @JsonProperty("data")
  public abstract ImmutableList<TopicAlterEntry> getData();

  @JsonProperty("validate_only")
  public abstract Optional<Boolean> getValidateOnly();

  public static AlterMultipleTopicsConfigsBatchRequestData create(
      List<TopicAlterEntry> data, Optional<Boolean> validateOnly) {
    return new AutoValue_AlterMultipleTopicsConfigsBatchRequestData(
        ImmutableList.copyOf(data), validateOnly);
  }

  @JsonCreator
  static AlterMultipleTopicsConfigsBatchRequestData fromJson(
      @JsonProperty("data") List<TopicAlterEntry> data,
      @JsonProperty("validate_only") Optional<Boolean> validateOnly) {
    return create(data, validateOnly);
  }

  public final Map<String, List<AlterConfigCommand>> toAlterConfigCommandsByTopic() {
    Map<String, List<AlterConfigCommand>> result = new LinkedHashMap<>();
    for (TopicAlterEntry entry : getData()) {
      List<AlterConfigCommand> commands = new ArrayList<>();
      for (AlterConfigBatchRequestData.AlterEntry configEntry : entry.getConfigs()) {
        switch (configEntry.getOperation()) {
          case SET:
            commands.add(
                AlterConfigCommand.set(configEntry.getName(), configEntry.getValue().orElse(null)));
            break;
          case DELETE:
            commands.add(AlterConfigCommand.delete(configEntry.getName()));
            break;
          default:
            throw new AssertionError("unreachable");
        }
      }
      result.put(entry.getTopicName(), unmodifiableList(commands));
    }
    return unmodifiableMap(result);
  }

  @AutoValue
  public abstract static class TopicAlterEntry {

    @JsonProperty("topic_name")
    public abstract String getTopicName();

    @JsonProperty("configs")
    public abstract ImmutableList<AlterConfigBatchRequestData.AlterEntry> getConfigs();

    public static TopicAlterEntry create(
        String topicName, List<AlterConfigBatchRequestData.AlterEntry> configs) {
      return new AutoValue_AlterMultipleTopicsConfigsBatchRequestData_TopicAlterEntry(
          topicName, ImmutableList.copyOf(configs));
    }

    @JsonCreator
    static TopicAlterEntry fromJson(
        @JsonProperty("topic_name") String topicName,
        @JsonProperty("configs") List<AlterConfigBatchRequestData.AlterEntry> configs) {
      return create(topicName, configs);
    }
  }
}
