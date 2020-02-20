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

package io.confluent.kafkarest.v2;

import io.confluent.kafkarest.ConsumerInstanceId;
import io.confluent.kafkarest.ConsumerRecordAndSize;
import io.confluent.kafkarest.KafkaRestConfig;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;


/**
 * Binary implementation of KafkaConsumerState that does no decoding, returning the raw bytes
 * directly.
 */
public class BinaryKafkaConsumerState extends KafkaConsumerState<byte[], byte[], byte[], byte[]> {

  public BinaryKafkaConsumerState(KafkaRestConfig config,
      ConsumerInstanceId instanceId,
      Consumer consumer) {
    super(config, instanceId, consumer);
  }

  @Override
  public ConsumerRecordAndSize<byte[], byte[]> createConsumerRecord(
      ConsumerRecord<byte[], byte[]> record) {
    long approxSize = (record.key() != null ? record.key().length : 0)
        + (record.value() != null ? record.value().length : 0);

    return new ConsumerRecordAndSize<byte[], byte[]>(
        new io.confluent.kafkarest.entities.ConsumerRecord<>(
            record.topic(), record.key(), record.value(), record.partition(), record.offset()),
        approxSize);
  }
}
