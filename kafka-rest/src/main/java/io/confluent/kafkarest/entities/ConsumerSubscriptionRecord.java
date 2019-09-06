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

package io.confluent.kafkarest.entities;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSetter;

import java.util.List;

public class ConsumerSubscriptionRecord {

  private String topicPattern;

  @JsonCreator
  public ConsumerSubscriptionRecord(
      @JsonProperty("topics") List<String> topics,
      @JsonProperty("topic_pattern") String topicPattern
  ) {
    this.topics = topics;
    this.topicPattern = topicPattern;
  }

  @JsonGetter("topic_pattern")
  public String getTopicPattern() {
    return this.topicPattern;
  }

  @JsonSetter("topic_pattern")
  public void setTopicPattern(String topicPattern) {
    this.topicPattern = topicPattern;
  }

  @JsonProperty
  public List<String> topics;
}
