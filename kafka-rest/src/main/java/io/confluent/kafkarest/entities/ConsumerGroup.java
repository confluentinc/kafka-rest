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

public class ConsumerGroup {

  private final String groupId;
  private final ConsumerGroupCoordinator coordinator;

  @JsonCreator
  public ConsumerGroup(@JsonProperty("groupId") String groupId,
                       @JsonProperty("coordinator") ConsumerGroupCoordinator coordinator) {
    this.groupId = groupId;
    this.coordinator = coordinator;
  }

  @JsonProperty
  public String getGroupId() {
    return groupId;
  }

  @JsonProperty
  public ConsumerGroupCoordinator getCoordinator() {
    return coordinator;
  }

  @Override
  public String toString() {
    return "ConsumerGroup{"
            + "groupId='" + groupId + '\''
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

    ConsumerGroup that = (ConsumerGroup) o;

    if (!groupId.equals(that.groupId)) {
      return false;
    }
    return coordinator.equals(that.coordinator);
  }

  @Override
  public int hashCode() {
    int result = groupId.hashCode();
    result = 31 * result + coordinator.hashCode();
    return result;
  }
}
