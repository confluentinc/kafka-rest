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
import java.util.List;
import java.util.Optional;
import javax.annotation.Nullable;
import org.apache.kafka.common.TopicPartition;

@AutoValue
public abstract class Partition {

  Partition() {
  }

  public abstract String getClusterId();

  public abstract String getTopicName();

  public abstract int getPartitionId();

  public abstract ImmutableList<PartitionReplica> getReplicas();

  @Nullable
  public abstract Long getEarliestOffset();

  @Nullable
  public abstract Long getLatestOffset();

  public final Optional<PartitionReplica> getLeader() {
    return getReplicas().stream().filter(PartitionReplica::isLeader).findAny();
  }

  public static Partition create(
      String clusterId,
      String topicName,
      int partitionId,
      List<PartitionReplica> replicas) {
    return create(
        clusterId,
        topicName,
        partitionId,
        replicas,
        /* earliestOffset= */ null,
        /* latestOffset= */ null);
  }

  public static Partition create(
      String clusterId,
      String topicName,
      int partitionId,
      List<PartitionReplica> replicas,
      @Nullable Long earliestOffset,
      @Nullable Long latestOffset) {
    return new AutoValue_Partition(
        clusterId,
        topicName,
        partitionId,
        ImmutableList.copyOf(replicas),
        earliestOffset,
        latestOffset);
  }

  public TopicPartition toTopicPartition() {
    return new TopicPartition(getTopicName(), getPartitionId());
  }
}
