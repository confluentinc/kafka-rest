/**
 * Copyright 2017 Confluent Inc.
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

package io.confluent.kafkarest.v2;

import io.confluent.kafkarest.ConsumerInstanceId;
import io.confluent.kafkarest.ConsumerRecordAndSize;
import io.confluent.kafkarest.KafkaRestConfig;
import io.confluent.kafkarest.entities.BinaryConsumerRecord;
import kafka.serializer.Decoder;
import kafka.serializer.DefaultDecoder;
import kafka.utils.VerifiableProperties;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;


/**
 * Binary implementation of KafkaConsumerState that does no decoding, returning the raw bytes
 * directly.
 */
public class BinaryKafkaConsumerState extends KafkaConsumerState<byte[], byte[], byte[], byte[]> {

  private static final Decoder<byte[]> decoder = new DefaultDecoder(new VerifiableProperties());

  public BinaryKafkaConsumerState(KafkaRestConfig config,
      ConsumerInstanceId instanceId,
      Consumer consumer) {
    super(config, instanceId, consumer);
  }

  @Override
  protected Decoder<byte[]> getKeyDecoder() {
    return decoder;
  }

  @Override
  protected Decoder<byte[]> getValueDecoder() {
    return decoder;
  }

  @Override
  public ConsumerRecordAndSize<byte[], byte[]> createConsumerRecord(
      ConsumerRecord<byte[], byte[]> record) {
    long approxSize = (record.key() != null ? record.key().length : 0)
        + (record.value() != null ? record.value().length : 0);

    return new ConsumerRecordAndSize<byte[], byte[]>(
        new BinaryConsumerRecord(record.topic(), record.key(), record.value(), record.partition(),
            record.offset()), approxSize);
  }
}
