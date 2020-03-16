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

import java.util.Objects;
import java.util.StringJoiner;
import javax.annotation.Nullable;

/**
 * A Kafka Topic Configuration.
 */
public final class TopicConfiguration {

  private final String clusterId;

  private final String topicName;

  private final String name;

  @Nullable
  private final String value;

  private final boolean isDefault;

  private final boolean isReadOnly;

  private final boolean isSensitive;

  public TopicConfiguration(
      String clusterId,
      String topicName,
      String name,
      @Nullable String value,
      boolean isDefault,
      boolean isReadOnly,
      boolean isSensitive) {
    this.clusterId = Objects.requireNonNull(clusterId);
    this.topicName = Objects.requireNonNull(topicName);
    this.name = Objects.requireNonNull(name);
    this.value = value;
    this.isDefault = isDefault;
    this.isReadOnly = isReadOnly;
    this.isSensitive = isSensitive;
  }

  public String getClusterId() {
    return clusterId;
  }

  public String getTopicName() {
    return topicName;
  }

  public String getName() {
    return name;
  }

  @Nullable
  public String getValue() {
    return value;
  }

  public boolean isDefault() {
    return isDefault;
  }

  public boolean isReadOnly() {
    return isReadOnly;
  }

  public boolean isSensitive() {
    return isSensitive;
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
    TopicConfiguration that = (TopicConfiguration) o;
    return isDefault == that.isDefault
        && isReadOnly == that.isReadOnly
        && isSensitive == that.isSensitive
        && Objects.equals(clusterId, that.clusterId)
        && Objects.equals(topicName, that.topicName)
        && Objects.equals(name, that.name)
        && Objects.equals(value, that.value);
  }
  // CHECKSTYLE:ON:CyclomaticComplexity

  @Override
  public int hashCode() {
    return Objects.hash(clusterId, topicName, name, value, isDefault, isSensitive);
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", TopicConfiguration.class.getSimpleName() + "[", "]")
        .add("clusterId='" + clusterId + "'")
        .add("topicName='" + topicName + "'")
        .add("name='" + name + "'")
        .add("value='" + value + "'")
        .add("isDefault=" + isDefault)
        .add("isSensitive=" + isSensitive)
        .toString();
  }
}
