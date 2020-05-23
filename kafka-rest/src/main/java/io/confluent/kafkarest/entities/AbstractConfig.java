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

import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;

import java.util.List;
import java.util.Objects;
import javax.annotation.Nullable;

/**
 * A Kafka config.
 */
public abstract class AbstractConfig {

  private final String clusterId;

  private final String name;

  @Nullable
  private final String value;

  private final boolean isDefault;

  private final boolean isReadOnly;

  private final boolean isSensitive;

  private final ConfigSource source;

  private final List<ConfigSynonym> synonyms;

  AbstractConfig(
      String clusterId,
      String name,
      @Nullable String value,
      boolean isDefault,
      boolean isReadOnly,
      boolean isSensitive,
      ConfigSource source,
      List<ConfigSynonym> synonyms) {
    this.clusterId = requireNonNull(clusterId);
    this.name = requireNonNull(name);
    this.value = value;
    this.isDefault = isDefault;
    this.isReadOnly = isReadOnly;
    this.isSensitive = isSensitive;
    this.source = requireNonNull(source);
    this.synonyms = requireNonNull(synonyms);
  }

  public final String getClusterId() {
    return clusterId;
  }

  public final String getName() {
    return name;
  }

  @Nullable
  public final String getValue() {
    return value;
  }

  public final boolean isDefault() {
    return isDefault;
  }

  public final boolean isReadOnly() {
    return isReadOnly;
  }

  public final boolean isSensitive() {
    return isSensitive;
  }

  public final ConfigSource getSource() {
    return source;
  }

  public final List<ConfigSynonym> getSynonyms() {
    return unmodifiableList(synonyms);
  }

  // CHECKSTYLE:OFF:CyclomaticComplexity
  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    AbstractConfig that = (AbstractConfig) o;
    return clusterId.equals(that.clusterId)
        && name.equals(that.name)
        && Objects.equals(value, that.value)
        && isDefault == that.isDefault
        && isReadOnly == that.isReadOnly
        && isSensitive == that.isSensitive
        && source.equals(that.source)
        && synonyms.equals(that.synonyms);
  }
  // CHECKSTYLE:ON:CyclomaticComplexity

  @Override
  public int hashCode() {
    return Objects.hash(
        clusterId, name, value, isDefault, isReadOnly, isSensitive, source, synonyms);
  }

  @Override
  public abstract String toString();
}
