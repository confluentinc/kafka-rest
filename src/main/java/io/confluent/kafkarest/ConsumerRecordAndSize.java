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

package io.confluent.kafkarest;

import io.confluent.kafkarest.entities.AbstractConsumerRecord;

/**
 * Simple wrapper for a AbstractConsumerRecord and an approximate serialized size.
 */
public class ConsumerRecordAndSize<K, V> {

  private final AbstractConsumerRecord<K, V> record;
  private final long size;

  public ConsumerRecordAndSize(AbstractConsumerRecord<K, V> record, long size) {
    this.record = record;
    this.size = size;
  }

  public AbstractConsumerRecord<K, V> getRecord() {
    return record;
  }

  public long getSize() {
    return size;
  }
}
