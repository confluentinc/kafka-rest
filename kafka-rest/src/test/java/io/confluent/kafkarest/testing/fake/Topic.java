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

import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;

final class Topic {
  private final ConcurrentMap<Integer, Partition> partitions = new ConcurrentHashMap<>();

  Topic(List<Partition> partitions) {
    partitions.forEach(partition -> this.partitions.put(partition.getPartitionId(), partition));
  }

  Partition getPartition(int partitionId) {
    Partition partition = partitions.get(partitionId);
    if (partition == null) {
      throw new UnknownTopicOrPartitionException(
          String.format("Partition %d doesn't exist.", partitionId));
    }
    return partition;
  }

  ImmutableList<Partition> getPartitions() {
    return ImmutableList.copyOf(partitions.values());
  }
}
