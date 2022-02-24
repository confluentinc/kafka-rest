/*
 * Copyright 2022 Confluent Inc.
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

package io.confluent.kafkarest.testing.fake;

import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableList;
import java.util.List;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;

final class Partition {
  private final String topicName;
  private final int partitionId;
  private final Replica leader;
  private final ImmutableList<Replica> followers;

  Partition(String topicName, int partitionId, Replica leader, List<Replica> followers) {
    this.topicName = requireNonNull(topicName);
    this.partitionId = partitionId;
    this.leader = requireNonNull(leader);
    this.followers = ImmutableList.copyOf(followers);
  }

  int getPartitionId() {
    return partitionId;
  }

  Replica getLeader() {
    return leader;
  }

  ImmutableList<Replica> getFollowers() {
    return followers;
  }

  ImmutableList<Replica> getReplicas() {
    return ImmutableList.<Replica>builder().add(leader).addAll(followers).build();
  }

  PartitionInfo toPartitionInfo(FakeKafkaCluster cluster) {
    Node[] replicas =
        getReplicas().stream()
            .map(Replica::getBrokerId)
            .map(cluster::getBroker)
            .map(Broker::toNode)
            .toArray(Node[]::new);
    return new PartitionInfo(
        topicName,
        partitionId,
        cluster.getBroker(leader.getBrokerId()).toNode(),
        replicas,
        replicas);
  }
}
