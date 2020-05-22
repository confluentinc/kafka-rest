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

import java.util.List;
import java.util.Objects;
import java.util.StringJoiner;
import javax.annotation.Nullable;

/**
 * A Kafka Broker Config
 */
public final class BrokerConfig extends AbstractConfig {

  private final int brokerId;

  public BrokerConfig(
      String clusterId,
      int brokerId,
      String name,
      @Nullable String value,
      boolean isDefault,
      boolean isReadOnly,
      boolean isSensitive,
      ConfigSource source,
      List<ConfigSynonym> synonyms) {
    super(clusterId, name, value, isDefault, isReadOnly, isSensitive, source, synonyms);
    this.brokerId = brokerId;
  }

  public int getBrokerId() {
    return brokerId;
  }

  @Override
  public boolean equals(Object o) {
    return super.equals(o) && brokerId == ((BrokerConfig) o).brokerId;
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), brokerId);
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", BrokerConfig.class.getSimpleName() + "[", "]")
        .add("clusterId='" + getClusterId() + "'")
        .add("brokerId=" + brokerId)
        .add("name='" + getName() + "'")
        .add("value='" + getValue() + "'")
        .add("isDefault=" + isDefault())
        .add("isSensitive=" + isSensitive())
        .add("source=" + getSource())
        .add("synonyms=" + getSynonyms())
        .toString();
  }
}
