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
import java.util.Optional;
import java.util.StringJoiner;

public final class Partition {

  private final String clusterId;

  private final String topicName;

  private final int partitionId;

  private final List<PartitionReplica> replicas;

  public Partition(
      String clusterId,
      String topicName,
      int partitionId,
      List<PartitionReplica> replicas) {
    this.clusterId = Objects.requireNonNull(clusterId);
    this.topicName = Objects.requireNonNull(topicName);
    this.partitionId = partitionId;
    this.replicas = Objects.requireNonNull(replicas);
  }

  public String getClusterId() {
    return clusterId;
  }

  public String getTopicName() {
    return topicName;
  }

  public int getPartitionId() {
    return partitionId;
  }

  public List<PartitionReplica> getReplicas() {
    return replicas;
  }

  public Optional<PartitionReplica> getLeader() {
    return replicas.stream().filter(PartitionReplica::isLeader).findAny();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Partition partition = (Partition) o;
    return partitionId == partition.partitionId
        && Objects.equals(clusterId, partition.clusterId)
        && Objects.equals(topicName, partition.topicName)
        && Objects.equals(replicas, partition.replicas);
  }

  @Override
  public int hashCode() {
    return Objects.hash(clusterId, topicName, partitionId, replicas);
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", Partition.class.getSimpleName() + "[", "]")
        .add("clusterId='" + clusterId + "'")
        .add("topicName='" + topicName + "'")
        .add("partitionId=" + partitionId)
        .add("replicas=" + replicas)
        .toString();
  }
}
