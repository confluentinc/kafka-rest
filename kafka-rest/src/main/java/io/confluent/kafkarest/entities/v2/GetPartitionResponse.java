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

package io.confluent.kafkarest.entities.v2;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.confluent.kafkarest.entities.Partition;
import io.confluent.kafkarest.entities.PartitionReplica;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.StringJoiner;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.validation.constraints.PositiveOrZero;

public final class GetPartitionResponse {

  @PositiveOrZero
  @Nullable
  private final Integer partition;

  @PositiveOrZero
  @Nullable
  private final Integer leader;

  @Nullable
  private final List<Replica> replicas;

  @JsonCreator
  private GetPartitionResponse(
      @JsonProperty("partition") @Nullable Integer partition,
      @JsonProperty("leader") @Nullable Integer leader,
      @JsonProperty("replicas") @Nullable List<Replica> replicas
  ) {
    this.partition = partition;
    this.leader = leader;
    this.replicas = replicas;
  }

  @JsonProperty
  @Nullable
  public Integer getPartition() {
    return partition;
  }

  @JsonProperty
  @Nullable
  public Integer getLeader() {
    return leader;
  }

  @JsonProperty
  @Nullable
  public List<Replica> getReplicas() {
    return replicas;
  }

  public static GetPartitionResponse fromPartition(Partition partition) {
    List<PartitionReplica> replicas =
        partition.getReplicas() != null ? partition.getReplicas() : Collections.emptyList();
    return new GetPartitionResponse(
        partition.getPartitionId(),
        partition.getLeader().map(PartitionReplica::getBrokerId).orElse(-1),
        replicas.stream().map(Replica::fromPartitionReplica).collect(Collectors.toList()));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    GetPartitionResponse that = (GetPartitionResponse) o;
    return Objects.equals(partition, that.partition)
        && Objects.equals(leader, that.leader)
        && Objects.equals(replicas, that.replicas);
  }

  @Override
  public int hashCode() {
    return Objects.hash(partition, leader, replicas);
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", GetPartitionResponse.class.getSimpleName() + "[", "]")
        .add("partition=" + partition)
        .add("leader=" + leader)
        .add("replicas=" + replicas)
        .toString();
  }

  public static final class Replica {

    @PositiveOrZero
    @Nullable
    private Integer broker;

    @Nullable
    private Boolean leader;

    @Nullable
    private Boolean inSync;

    @JsonCreator
    private Replica(
        @JsonProperty("broker") @Nullable Integer broker,
        @JsonProperty("leader") @Nullable Boolean leader,
        @JsonProperty("in_sync") @Nullable Boolean inSync) {
      this.broker = broker;
      this.leader = leader;
      this.inSync = inSync;
    }

    @JsonProperty("broker")
    @Nullable
    public Integer getBroker() {
      return broker;
    }

    @JsonProperty("leader")
    @Nullable
    public Boolean getLeader() {
      return leader;
    }

    @JsonProperty("in_sync")
    @Nullable
    public Boolean getInSync() {
      return inSync;
    }

    public static Replica fromPartitionReplica(PartitionReplica replica) {
      return new Replica(replica.getBrokerId(), replica.isLeader(), replica.isInSync());
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      Replica that = (Replica) o;
      return Objects.equals(broker, that.broker)
          && Objects.equals(leader, that.leader)
          && Objects.equals(inSync, that.inSync);
    }

    @Override
    public int hashCode() {
      return Objects.hash(broker, leader, inSync);
    }

    @Override
    public String toString() {
      return new StringJoiner(", ", Replica.class.getSimpleName() + "[", "]")
          .add("broker=" + broker)
          .add("leader=" + leader)
          .add("inSync=" + inSync)
          .toString();
    }
  }
}
