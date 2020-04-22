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

import java.util.Objects;
import java.util.StringJoiner;

public final class PartitionReplica {

  private final String clusterId;

  private final String topicName;

  private final int partitionId;

  private final int brokerId;

  private final boolean isLeader;

  private final boolean isInSync;

  public PartitionReplica(
      String clusterId,
      String topicName,
      int partitionId,
      int brokerId,
      boolean isLeader,
      boolean isInSync
  ) {
    this.clusterId = Objects.requireNonNull(clusterId);
    this.topicName = Objects.requireNonNull(topicName);
    this.partitionId = partitionId;
    this.brokerId = brokerId;
    this.isLeader = isLeader;
    this.isInSync = isInSync;
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

  public int getBrokerId() {
    return brokerId;
  }

  public boolean isLeader() {
    return isLeader;
  }

  public boolean isInSync() {
    return isInSync;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    PartitionReplica that = (PartitionReplica) o;
    return partitionId == that.partitionId
        && brokerId == that.brokerId
        && isLeader == that.isLeader
        && isInSync == that.isInSync
        && Objects.equals(clusterId, that.clusterId)
        && Objects.equals(topicName, that.topicName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(clusterId, topicName, partitionId, brokerId, isLeader, isInSync);
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", PartitionReplica.class.getSimpleName() + "[", "]")
        .add("clusterId='" + clusterId + "'")
        .add("topicName='" + topicName + "'")
        .add("partitionId=" + partitionId)
        .add("brokerId=" + brokerId)
        .add("isLeader=" + isLeader)
        .add("isInSync=" + isInSync)
        .toString();
  }
}
