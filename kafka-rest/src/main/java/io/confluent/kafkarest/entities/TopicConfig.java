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

import java.util.List;
import java.util.Objects;
import java.util.StringJoiner;
import javax.annotation.Nullable;

/**
 * A Kafka Topic Config.
 */
public final class TopicConfig extends AbstractConfig {

  private final String topicName;

  public TopicConfig(
      String clusterId,
      String topicName,
      String name,
      @Nullable String value,
      boolean isDefault,
      boolean isReadOnly,
      boolean isSensitive,
      ConfigSource source,
      List<ConfigSynonym> synonyms) {
    super(clusterId, name, value, isDefault, isReadOnly, isSensitive, source, synonyms);
    this.topicName = requireNonNull(topicName);
  }

  public String getTopicName() {
    return topicName;
  }

  @Override
  public boolean equals(Object o) {
    return super.equals(o) && topicName.equals(((TopicConfig) o).topicName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), topicName);
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", TopicConfig.class.getSimpleName() + "[", "]")
        .add("clusterId='" + getClusterId() + "'")
        .add("topicName=" + topicName)
        .add("name='" + getName() + "'")
        .add("value='" + getValue() + "'")
        .add("isDefault=" + isDefault())
        .add("isSensitive=" + isSensitive())
        .add("source=" + getSource())
        .add("synonyms=" + getSynonyms())
        .toString();
  }
}
