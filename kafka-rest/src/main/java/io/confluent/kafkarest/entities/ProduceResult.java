/*
 * Copyright 2021 Confluent Inc.
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
import org.apache.kafka.clients.producer.RecordMetadata;

@AutoValue
public abstract class ProduceResult {

  ProduceResult() {
  }

  public abstract int getPartitionId();

  public abstract long getOffset();

  public static ProduceResult create(int partitionId, long offset) {
    return new AutoValue_ProduceResult(partitionId, offset);
  }

  public static ProduceResult fromRecordMetadata(RecordMetadata metadata) {
    return create(metadata.partition(), metadata.offset());
  }
}
