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

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Collection;

import io.confluent.kafkarest.entities.ProduceRecord;

/**
 * Wrapper producer for content types which have no associated schema (e.g. binary or JSON).
 */
public class NoSchemaRestProducer<K, V> implements RestProducer<K, V> {

  private KafkaProducer<K, V> producer;

  public NoSchemaRestProducer(KafkaProducer<K, V> producer) {
    this.producer = producer;
  }

  @Override
  public void produce(
      ProduceTask task,
      String topic,
      Integer partition,
      Collection<? extends ProduceRecord<K, V>> produceRecords
  ) {
    for (ProduceRecord<K, V> record : produceRecords) {
      Integer recordPartition = partition;
      if (recordPartition == null) {
        recordPartition = record.partition();
      }
      producer.send(
          new ProducerRecord(topic, recordPartition, record.getKey(), record.getValue()),
          task.createCallback()
      );
    }
  }

  @Override
  public void close() {
    producer.close();
  }
}
