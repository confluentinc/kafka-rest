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
import javax.annotation.Nullable;

public final class Partition {

  private int partition;

  private int leader;

  @Nullable
  private List<PartitionReplica> replicas;

  public Partition() {
  }

  public Partition(int partition, int leader, @Nullable List<PartitionReplica> replicas) {
    this.partition = partition;
    this.leader = leader;
    this.replicas = replicas;
  }

  public int getPartition() {
    return partition;
  }

  public void setPartition(int partition) {
    this.partition = partition;
  }

  public int getLeader() {
    return leader;
  }

  public void setLeader(int leader) {
    this.leader = leader;
  }

  @Nullable
  public List<PartitionReplica> getReplicas() {
    return replicas;
  }

  public void setReplicas(@Nullable List<PartitionReplica> replicas) {
    this.replicas = replicas;
  }
}
