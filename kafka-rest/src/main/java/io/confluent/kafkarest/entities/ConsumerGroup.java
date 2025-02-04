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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.common.GroupState;

@AutoValue
public abstract class ConsumerGroup {

  ConsumerGroup() {}

  public abstract String getClusterId();

  public abstract String getConsumerGroupId();

  public abstract boolean isSimple();

  public abstract String getPartitionAssignor();

  public abstract State getState();

  public abstract Broker getCoordinator();

  public abstract ImmutableList<Consumer> getConsumers();

  public final ImmutableMap<Partition, Consumer> getPartitionAssignment() {
    ImmutableMap.Builder<Partition, Consumer> partitionAssignment = ImmutableMap.builder();
    for (Consumer consumer : getConsumers()) {
      for (Partition partition : consumer.getAssignedPartitions()) {
        partitionAssignment.put(partition, consumer);
      }
    }
    return partitionAssignment.build();
  }

  public static Builder builder() {
    return new AutoValue_ConsumerGroup.Builder();
  }

  public static ConsumerGroup fromConsumerGroupDescription(
      String clusterId, ConsumerGroupDescription description) {
    return builder()
        .setClusterId(clusterId)
        .setConsumerGroupId(description.groupId())
        .setSimple(description.isSimpleConsumerGroup())
        // I have only seen partitionAssignor="" in all my tests, no matter what I do.
        // TODO: Investigate how to actually get partition assignor.
        .setPartitionAssignor(description.partitionAssignor())
        // I have only been able to see state=PREPARING_REBALANCE on all my tests.
        // TODO: Investigate how to get actual state of consumer group.
        .setState(new State(description.groupState()))
        .setCoordinator(Broker.fromNode(clusterId, description.coordinator()))
        .setConsumers(
            description.members().stream()
                .map(
                    consumer ->
                        Consumer.fromMemberDescription(clusterId, description.groupId(), consumer))
                .collect(Collectors.toList()))
        .build();
  }

  @AutoValue.Builder
  public abstract static class Builder {

    Builder() {}

    public abstract Builder setClusterId(String clusterId);

    public abstract Builder setConsumerGroupId(String consumerGroupId);

    public abstract Builder setSimple(boolean isSimple);

    public abstract Builder setPartitionAssignor(String partitionAssignor);

    public abstract Builder setState(State state);

    public abstract Builder setCoordinator(Broker coordinator);

    public abstract Builder setConsumers(List<Consumer> consumers);

    public abstract ConsumerGroup build();
  }

  /**
   * Encapsulates the GroupState enum to provide a JSON serde format that: serialize to
   * SCREAMING_SNAKE_CASE of state name and deserialize from SCREAMING_SNAKE_CASE of state name
   */
  public static class State {
    public static final State UNKNOWN = new State(GroupState.UNKNOWN);
    public static final State PREPARING_REBALANCE = new State(GroupState.PREPARING_REBALANCE);
    public static final State COMPLETING_REBALANCE = new State(GroupState.COMPLETING_REBALANCE);
    public static final State STABLE = new State(GroupState.STABLE);
    public static final State DEAD = new State(GroupState.DEAD);
    public static final State EMPTY = new State(GroupState.EMPTY);

    // map of name (in SCREAMING_SNAKE_CASE) to enum GroupState
    private static final Map<String, GroupState> NAME_TO_ENUM =
        Arrays.stream(GroupState.values())
            .collect(
                Collectors.toMap(
                    state -> state.name().toUpperCase(Locale.ROOT), Function.identity()));

    private final GroupState state;

    State(GroupState state) {
      this.state = state;
    }

    public GroupState toGroupState() {
      return state;
    }

    @JsonValue
    @Override
    public String toString() {
      return state.name().toUpperCase(Locale.ROOT);
    }

    @JsonCreator
    public static State fromString(String state) {
      return new State(
          NAME_TO_ENUM.getOrDefault(state.toUpperCase(Locale.ROOT), GroupState.UNKNOWN));
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null || getClass() != obj.getClass()) {
        return false;
      }
      State that = (State) obj;
      return state == that.state;
    }
  }
}
