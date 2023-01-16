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
import io.confluent.kafkarest.entities.BrokerConfig;
import io.confluent.kafkarest.entities.ConfigSource;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

@AutoValue
public abstract class BrokerConfigData extends AbstractConfigData {

  BrokerConfigData() {
  }

  @JsonProperty("broker_id")
  public abstract int getBrokerId();

  public static Builder builder() {
    return new AutoValue_BrokerConfigData.Builder().setKind("KafkaBrokerConfig");
  }

  public static Builder fromBrokerConfig(BrokerConfig config) {
    return builder()
        .setClusterId(config.getClusterId())
        .setBrokerId(config.getBrokerId())
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
  static BrokerConfigData fromJson(
      @JsonProperty("kind") String kind,
      @JsonProperty("metadata") Metadata metadata,
      @JsonProperty("cluster_id") String clusterId,
      @JsonProperty("broker_id") int brokerId,
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
        .setBrokerId(brokerId)
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

    public abstract Builder setBrokerId(int brokerId);

    public abstract BrokerConfigData build();
  }
}
