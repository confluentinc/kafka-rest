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
import io.confluent.kafkarest.entities.ConfigSynonym;
import java.util.Optional;
import javax.annotation.Nullable;

@AutoValue
public abstract class ConfigSynonymData {

  ConfigSynonymData() {
  }

  @JsonProperty("name")
  public abstract String getName();

  @JsonProperty("value")
  public abstract Optional<String> getValue();

  @JsonProperty("source")
  public abstract ConfigSource getSource();

  public static Builder builder() {
    return new AutoValue_ConfigSynonymData.Builder();
  }

  public static ConfigSynonymData fromConfigSynonym(ConfigSynonym synonym) {
    return builder()
        .setName(synonym.getName())
        .setValue(synonym.getValue())
        .setSource(synonym.getSource())
        .build();
  }

  @JsonCreator
  static ConfigSynonymData fromJson(
      @JsonProperty("name") String name,
      @JsonProperty("value") @Nullable String value,
      @JsonProperty("source") ConfigSource source
  ) {
    return builder()
        .setName(name)
        .setValue(value)
        .setSource(source)
        .build();
  }

  @AutoValue.Builder
  public abstract static class Builder {

    Builder() {
    }

    public abstract Builder setName(String name);

    public abstract Builder setValue(@Nullable String value);

    public abstract Builder setSource(ConfigSource source);

    public abstract ConfigSynonymData build();
  }
}
