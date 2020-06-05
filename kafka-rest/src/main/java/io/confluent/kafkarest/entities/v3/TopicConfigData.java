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
import io.confluent.kafkarest.entities.ConfigSource;
import io.confluent.kafkarest.entities.TopicConfig;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

@AutoValue
public abstract class TopicConfigData extends AbstractConfigData {

  TopicConfigData() {
  }

  @JsonProperty("topic_name")
  public abstract String getTopicName();

  public static Builder builder() {
    return new AutoValue_TopicConfigData.Builder().setKind("KafkaTopicConfig");
  }

  public static Builder fromTopicConfig(TopicConfig config) {
    return builder()
        .setClusterId(config.getClusterId())
        .setTopicName(config.getTopicName())
        .setName(config.getName())
        .setValue(config.getValue())
        .setDefault(config.isDefault())
        .setReadOnly(config.isReadOnly())
        .setSensitive(config.isSensitive())
        .setSource(config.getSource())
        .setSynonyms(
            config.getSynonyms()
                .stream()
                .map(ConfigSynonymData::fromConfigSynonym)
                .collect(Collectors.toList()));
  }

  // CHECKSTYLE:OFF:ParameterNumber
  @JsonCreator
  static TopicConfigData fromJson(
      @JsonProperty("kind") String kind,
      @JsonProperty("metadata") Metadata metadata,
      @JsonProperty("cluster_id") String clusterId,
      @JsonProperty("topic_name") String topicName,
      @JsonProperty("name") String name,
      @JsonProperty("value") @Nullable String value,
      @JsonProperty("is_default") boolean isDefault,
      @JsonProperty("is_read_only") boolean isReadOnly,
      @JsonProperty("is_sensitive") boolean isSensitive,
      @JsonProperty("source") ConfigSource source,
      @JsonProperty("synonyms") List<ConfigSynonymData> synonyms
  ) {
    return builder()
        .setKind(kind)
        .setMetadata(metadata)
        .setClusterId(clusterId)
        .setTopicName(topicName)
        .setName(name)
        .setValue(value)
        .setDefault(isDefault)
        .setReadOnly(isReadOnly)
        .setSensitive(isSensitive)
        .setSource(source)
        .setSynonyms(synonyms)
        .build();
  }
  // CHECKSTYLE:ON:ParameterNumber

  @AutoValue.Builder
  public abstract static class Builder extends AbstractConfigData.Builder<Builder> {

    Builder() {
    }

    public abstract Builder setTopicName(String topicName);

    public abstract TopicConfigData build();
  }
}
