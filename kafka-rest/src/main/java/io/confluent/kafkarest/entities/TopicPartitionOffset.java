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

import com.google.auto.value.AutoValue;

@AutoValue
public abstract class TopicPartitionOffset {

  TopicPartitionOffset() {
  }

  public abstract String getTopic();

  public abstract int getPartition();

  public abstract long getConsumed();

  public abstract long getCommitted();

  public static TopicPartitionOffset create(
      String topic, int partition, long consumed, long committed) {
    if (topic.isEmpty()) {
      throw new IllegalArgumentException();
    }
    return new AutoValue_TopicPartitionOffset(topic, partition, consumed, committed);
  }
}
