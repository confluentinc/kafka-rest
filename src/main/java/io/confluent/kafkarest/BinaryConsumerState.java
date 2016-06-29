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

import io.confluent.kafkarest.entities.BinaryConsumerRecord;
import kafka.serializer.DefaultDecoder;
import kafka.utils.VerifiableProperties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Properties;

/**
 * Binary implementation of ConsumerState that does no decoding, returning the raw bytes directly.
 */
public class BinaryConsumerState extends ConsumerState<byte[], byte[], byte[], byte[]> {

  private static final Deserializer<byte[]> deserializer = new ByteArrayDeserializer();

  public BinaryConsumerState(KafkaRestConfig config, ConsumerInstanceId instanceId,
                             Properties consumerProperties,
                             ConsumerManager.ConsumerFactory consumerFactory) {
    super(config, instanceId, consumerProperties, consumerFactory);
  }

  @Override
  protected Deserializer<byte[]> getKeyDeserializer() {
    return deserializer;
  }

  @Override
  protected Deserializer<byte[]> getValueDeserializer() {
    return deserializer;
  }

  @Override
  public ConsumerRecordAndSize<byte[], byte[]> convertConsumerRecord(
      ConsumerRecord<byte[], byte[]> msg) {
    long approxSize = (msg.key() != null ? msg.key().length : 0)
                      + (msg.value() != null ? msg.value().length : 0);
    return new ConsumerRecordAndSize<byte[], byte[]>(
        new BinaryConsumerRecord(msg.key(), msg.value(), msg.topic(), msg.partition(), msg.offset()),
        approxSize);
  }

}
