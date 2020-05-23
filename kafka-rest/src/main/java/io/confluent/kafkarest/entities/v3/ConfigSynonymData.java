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

import com.fasterxml.jackson.annotation.JsonProperty;
import io.confluent.kafkarest.entities.ConfigSource;
import io.confluent.kafkarest.entities.ConfigSynonym;
import java.util.Objects;
import java.util.StringJoiner;
import javax.annotation.Nullable;

public final class ConfigSynonymData {

  private final String name;

  @Nullable
  private final String value;

  private final ConfigSource source;

  public ConfigSynonymData(
      @JsonProperty("name") String name,
      @JsonProperty("value") @Nullable String value,
      @JsonProperty("source") ConfigSource source) {
    this.name = name;
    this.value = value;
    this.source = source;
  }

  public static ConfigSynonymData fromConfigSynonym(ConfigSynonym synonym) {
    return new ConfigSynonymData(synonym.getName(), synonym.getValue(), synonym.getSource());
  }

  @JsonProperty("name")
  public String getName() {
    return name;
  }

  @JsonProperty("value")
  @Nullable
  public String getValue() {
    return value;
  }

  @JsonProperty("source")
  public ConfigSource getSource() {
    return source;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ConfigSynonymData that = (ConfigSynonymData) o;
    return name.equals(that.name) && Objects.equals(value, that.value) && source == that.source;
  }

  @Override
  public int hashCode() {
    return Objects.hash(getName(), getValue(), getSource());
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", ConfigSynonymData.class.getSimpleName() + "[", "]")
        .add("name='" + name + "'")
        .add("value='" + value + "'")
        .add("source=" + source)
        .toString();
  }
}
