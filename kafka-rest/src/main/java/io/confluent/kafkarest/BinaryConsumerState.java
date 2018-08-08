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
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import kafka.serializer.Decoder;
import kafka.serializer.DefaultDecoder;
import kafka.utils.VerifiableProperties;

/**
 * Binary implementation of ConsumerState that does no decoding, returning the raw bytes directly.
 */
public class BinaryConsumerState extends ConsumerState<byte[], byte[], byte[], byte[]> {

  private static final Decoder<byte[]> decoder = new DefaultDecoder(new VerifiableProperties());

  public BinaryConsumerState(KafkaRestConfig config,
                             ConsumerInstanceId instanceId,
                             ConsumerConnector consumer) {
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
      MessageAndMetadata<byte[], byte[]> msg) {
    long approxSize = (msg.key() != null ? msg.key().length : 0)
                      + (msg.message() != null ? msg.message().length : 0);
    return new ConsumerRecordAndSize<>(
        new BinaryConsumerRecord(msg.topic(),
                                 msg.key(),
                                 msg.message(),
                                 msg.partition(),
                                 msg.offset()),
        approxSize);
  }

}
