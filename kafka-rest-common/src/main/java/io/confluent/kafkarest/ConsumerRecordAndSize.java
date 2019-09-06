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

package io.confluent.kafkarest;

import io.confluent.kafkarest.entities.ConsumerRecord;

/**
 * Simple wrapper for a ConsumerRecord and an approximate serialized size.
 */
public class ConsumerRecordAndSize<K, V> {

  private final ConsumerRecord<K, V> record;
  private final long size;

  public ConsumerRecordAndSize(ConsumerRecord<K, V> record, long size) {
    this.record = record;
    this.size = size;
  }

  public ConsumerRecord<K, V> getRecord() {
    return record;
  }

  public long getSize() {
    return size;
  }
}
