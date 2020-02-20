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

import java.util.List;
import java.util.Objects;
import java.util.StringJoiner;
import javax.annotation.Nullable;

public final class Partition {

  private final int partition;

  private final int leader;

  @Nullable
  private List<PartitionReplica> replicas;

  public Partition(int partition, int leader, @Nullable List<PartitionReplica> replicas) {
    this.partition = partition;
    this.leader = leader;
    this.replicas = replicas;
  }

  public int getPartition() {
    return partition;
  }

  public int getLeader() {
    return leader;
  }

  @Nullable
  public List<PartitionReplica> getReplicas() {
    return replicas;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Partition partition1 = (Partition) o;
    return partition == partition1.partition
        && leader == partition1.leader
        && Objects.equals(replicas, partition1.replicas);
  }

  @Override
  public int hashCode() {
    return Objects.hash(partition, leader, replicas);
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", Partition.class.getSimpleName() + "[", "]")
        .add("partition=" + partition)
        .add("leader=" + leader)
        .add("replicas=" + replicas)
        .toString();
  }
}
