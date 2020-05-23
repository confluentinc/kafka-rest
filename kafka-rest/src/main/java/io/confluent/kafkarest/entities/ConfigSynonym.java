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

import static java.util.Objects.requireNonNull;

import java.util.Objects;
import java.util.StringJoiner;
import javax.annotation.Nullable;
import org.apache.kafka.clients.admin.ConfigEntry;

/**
 * Synonym of a {@link AbstractConfig config}.
 *
 * @see org.apache.kafka.clients.admin.ConfigEntry.ConfigSynonym
 * @see <a href=
 *      https://cwiki.apache.org/confluence/display/KAFKA/KIP-226+-+Dynamic+Broker+Configuration">
 *      KIP-226 - Dynamic Broker Configuration</a>
 */
public final class ConfigSynonym {

  private final String name;

  @Nullable
  private final String value;

  private final ConfigSource source;

  public ConfigSynonym(String name, @Nullable String value, ConfigSource source) {
    this.name = requireNonNull(name);
    this.value = value;
    this.source = requireNonNull(source);
  }

  public static ConfigSynonym fromAdminConfigSynonym(ConfigEntry.ConfigSynonym synonym) {
    return new ConfigSynonym(
        synonym.name(), synonym.value(), ConfigSource.fromAdminConfigSource(synonym.source()));
  }

  public String getName() {
    return name;
  }

  @Nullable
  public String getValue() {
    return value;
  }

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
    ConfigSynonym synonym = (ConfigSynonym) o;
    return name.equals(synonym.name)
        && Objects.equals(value, synonym.value)
        && source == synonym.source;
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, value, source);
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", ConfigSynonym.class.getSimpleName() + "[", "]")
        .add("name='" + name + "'")
        .add("value='" + value + "'")
        .add("source=" + source)
        .toString();
  }
}
