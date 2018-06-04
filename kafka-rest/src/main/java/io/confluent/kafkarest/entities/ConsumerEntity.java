/*
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.kafkarest.entities;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collections;
import java.util.List;

public class ConsumerEntity {

  public static ConsumerEntity empty() {
    return new ConsumerEntity(Collections.<TopicPartitionEntity>emptyList(), 0,
            ConsumerGroupCoordinator.empty());
  }

  private final List<TopicPartitionEntity> topicPartitionList;
  private final Integer topicPartitionCount;
  private final ConsumerGroupCoordinator coordinator;

  @JsonCreator
  public ConsumerEntity(
          @JsonProperty("topicPartitionList") List<TopicPartitionEntity> topicPartitionList,
          @JsonProperty("topicPartitionCount") Integer topicPartitionCount,
          @JsonProperty("coordinator") ConsumerGroupCoordinator coordinator) {
    this.topicPartitionList = topicPartitionList;
    this.topicPartitionCount = topicPartitionCount;
    this.coordinator = coordinator;
  }

  @JsonProperty
  public List<TopicPartitionEntity> getTopicPartitionList() {
    return topicPartitionList;
  }

  @JsonProperty
  public Integer getTopicPartitionCount() {
    return topicPartitionCount;
  }

  @JsonProperty
  public ConsumerGroupCoordinator getCoordinator() {
    return coordinator;
  }

  @Override
  public String toString() {
    return "ConsumerEntity{"
            + "topicPartitionList=" + topicPartitionList
            + ", topicPartitionCount=" + topicPartitionCount
            + ", coordinator=" + coordinator
            + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ConsumerEntity that = (ConsumerEntity) o;

    if (!topicPartitionList.equals(that.topicPartitionList)) {
      return false;
    }
    if (!topicPartitionCount.equals(that.topicPartitionCount)) {
      return false;
    }
    return coordinator.equals(that.coordinator);
  }

  @Override
  public int hashCode() {
    int result = topicPartitionList.hashCode();
    result = 31 * result + topicPartitionCount.hashCode();
    result = 31 * result + coordinator.hashCode();
    return result;
  }
}
