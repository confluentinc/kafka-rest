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

import java.util.Objects;
import java.util.StringJoiner;

public final class TopicPartitionOffset {

  private final String topic;

  private final int partition;

  private final long consumed;

  private final long committed;

  public TopicPartitionOffset(String topic, int partition, long consumed, long committed) {
    if (topic.isEmpty()) {
      throw new IllegalArgumentException();
    }
    this.topic = topic;
    this.partition = partition;
    this.consumed = consumed;
    this.committed = committed;
  }

  public String getTopic() {
    return topic;
  }

  public int getPartition() {
    return partition;
  }

  public long getConsumed() {
    return consumed;
  }

  public long getCommitted() {
    return committed;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    TopicPartitionOffset that = (TopicPartitionOffset) o;
    return partition == that.partition
        && consumed == that.consumed
        && committed == that.committed
        && Objects.equals(topic, that.topic);
  }

  @Override
  public int hashCode() {
    return Objects.hash(topic, partition, consumed, committed);
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", TopicPartitionOffset.class.getSimpleName() + "[", "]")
        .add("topic='" + topic + "'")
        .add("partition=" + partition)
        .add("consumed=" + consumed)
        .add("committed=" + committed)
        .toString();
  }
}
