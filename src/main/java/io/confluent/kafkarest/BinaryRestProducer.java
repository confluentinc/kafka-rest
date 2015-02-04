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
import org.apache.kafka.common.serialization.Serializer;

import java.util.Collection;

import io.confluent.kafkarest.entities.ProduceRecord;

/**
 * Wrapper for KafkaProducers that handles schemas.
 */
public class BinaryRestProducer implements RestProducer<byte[], byte[]> {

  protected final KafkaProducer<byte[], byte[]> producer;
  protected final Serializer<byte[]> keySerializer;
  protected final Serializer<byte[]> valueSerializer;

  public BinaryRestProducer(KafkaProducer<byte[], byte[]> producer,
                            Serializer<byte[]> keySerializer,
                            Serializer<byte[]> valueSerializer) {
    this.producer = producer;
    this.keySerializer = keySerializer;
    this.valueSerializer = valueSerializer;
  }

  public void produce(ProduceTask task, String topic, Integer partition,
                      Collection<? extends ProduceRecord<byte[], byte[]>> records) {
    for (ProduceRecord<byte[], byte[]> record : records) {
      producer.send(new ProducerRecord(topic, partition, record.getKey(), record.getValue()), task);
    }
  }

  public void close() {
    producer.close();
  }
}
