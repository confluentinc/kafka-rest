/**
 * Copyright 2015 Confluent Inc.
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
 **/

package io.confluent.kafkarest.entities;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

import javax.validation.constraints.Min;

public class Partition {

  @Min(0)
  private int partition;
  @Min(0)
  private int leader;
  private List<PartitionReplica> replicas;

  public Partition() {
  }

  public Partition(
      @JsonProperty int partition, @JsonProperty int leader,
      @JsonProperty List<PartitionReplica> replicas
  ) {
    this.partition = partition;
    this.leader = leader;
    this.replicas = replicas;
  }

  @JsonProperty
  public int getPartition() {
    return partition;
  }

  @JsonProperty
  public void setPartition(int partition) {
    this.partition = partition;
  }

  @JsonProperty
  public int getLeader() {
    return leader;
  }

  @JsonProperty
  public void setLeader(int leader) {
    this.leader = leader;
  }

  @JsonProperty
  public List<PartitionReplica> getReplicas() {
    return replicas;
  }

  @JsonProperty
  public void setReplicas(List<PartitionReplica> replicas) {
    this.replicas = replicas;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof Partition)) {
      return false;
    }

    Partition partition1 = (Partition) o;

    if (leader != partition1.leader) {
      return false;
    }
    if (partition != partition1.partition) {
      return false;
    }
    if (replicas != null ? !replicas.equals(partition1.replicas) : partition1.replicas != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int result = partition;
    result = 31 * result + leader;
    result = 31 * result + (replicas != null ? replicas.hashCode() : 0);
    return result;
  }

  @Override
  public String toString() {
    return "Partition{"
           + "partition=" + partition
           + ", leader=" + leader
           + ", replicas=" + replicas
           + '}';
  }
}
