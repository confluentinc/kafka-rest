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

import java.util.Collection;

import io.confluent.kafkarest.entities.ProduceRecord;

/**
 * Wrapper for KafkaProducer that handles schemas.
 */
public interface RestProducer<K, V> {

  /**
   * Produces messages to the topic, handling any conversion, schema lookups or other operations
   * that need to be performed before sending the messages. If schemas are looked up or registered,
   * the SchemaHolder is updated with the resulting IDs.
   */
  public void produce(ProduceTask task, String topic, Integer partition,
                      Collection<? extends ProduceRecord<K, V>> records);

  public void close();
}
