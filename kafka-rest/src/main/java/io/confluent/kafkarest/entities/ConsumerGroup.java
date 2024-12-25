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

import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Map;
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
        .setState(State.fromConsumerGroupState(description.groupState()))
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

  public enum State {
    UNKNOWN,

    PREPARING_REBALANCE,

    COMPLETING_REBALANCE,

    STABLE,

    DEAD,

    EMPTY,

    ASSIGNING,

    RECONCILING;

    private static final Map<State, GroupState> STATE_TO_GROUP_STATE =
        ImmutableMap.<State, GroupState>builder()
            .put(UNKNOWN, GroupState.UNKNOWN)
            .put(PREPARING_REBALANCE, GroupState.PREPARING_REBALANCE)
            .put(COMPLETING_REBALANCE, GroupState.COMPLETING_REBALANCE)
            .put(STABLE, GroupState.STABLE)
            .put(DEAD, GroupState.DEAD)
            .put(EMPTY, GroupState.EMPTY)
            .put(ASSIGNING, GroupState.ASSIGNING)
            .put(RECONCILING, GroupState.RECONCILING)
            .build();

    public static State fromConsumerGroupState(GroupState state) {
      try {
        return State.valueOf(state.name());
      } catch (IllegalArgumentException e) {
        return UNKNOWN;
      }
    }

    public GroupState toConsumerGroupState() {
      GroupState groupState = STATE_TO_GROUP_STATE.get(this);
      return groupState == null ? GroupState.UNKNOWN : groupState;
    }
  }
}
