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

import io.confluent.kafkarest.entities.ForwardHeader;
import io.confluent.kafkarest.entities.ProduceRecord;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;

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
        recordPartition = record.getPartition();
      }
      List<Header> headers = null;
      if (record.getHeaders() != null && record.getHeaders().size() > 0) {
        headers = record
                .getHeaders()
                .stream()
                .filter(m -> m.value != null && m.value.length > 0)
                .map(ForwardHeader::toHeader)
                .collect(Collectors.toList());
      }
      producer.send(
          new ProducerRecord<>(topic, recordPartition, record.getKey(), record.getValue(), headers),
          task.createCallback()
      );
    }
  }

  @Override
  public void close() {
    producer.close();
  }
}
