/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.kafkarest.entities.v2;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.Objects;
import java.util.StringJoiner;
import javax.annotation.Nullable;

public final class ConsumerSubscriptionRecord {

  @Nullable
  private final List<String> topics;

  @Nullable
  private final String topicPattern;

  @JsonCreator
  public ConsumerSubscriptionRecord(
      @JsonProperty("topics") @Nullable List<String> topics,
      @JsonProperty("topic_pattern") @Nullable String topicPattern
  ) {
    this.topics = topics;
    this.topicPattern = topicPattern;
  }

  @JsonProperty
  @Nullable
  public List<String> getTopics() {
    return topics;
  }

  @JsonProperty("topic_pattern")
  @Nullable
  public String getTopicPattern() {
    return this.topicPattern;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ConsumerSubscriptionRecord that = (ConsumerSubscriptionRecord) o;
    return Objects.equals(topics, that.topics) && Objects.equals(topicPattern, that.topicPattern);
  }

  @Override
  public int hashCode() {
    return Objects.hash(topics, topicPattern);
  }

  @Override
  public String toString() {
    return new StringJoiner(
        ", ", ConsumerSubscriptionRecord.class.getSimpleName() + "[", "]")
        .add("topics=" + topics)
        .add("topicPattern='" + topicPattern + "'")
        .toString();
  }
}
