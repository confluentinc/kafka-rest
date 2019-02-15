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

import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.kafka.common.errors.SerializationException;

import io.confluent.kafkarest.entities.JsonConsumerRecord;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import kafka.serializer.Decoder;
import kafka.serializer.DefaultDecoder;
import kafka.utils.VerifiableProperties;

public class JsonConsumerState extends ConsumerState<byte[], byte[], Object, Object> {

  private static final Decoder<byte[]> decoder = new DefaultDecoder(new VerifiableProperties());
  private static final ObjectMapper objectMapper = new ObjectMapper();

  public JsonConsumerState(
      KafkaRestConfig config,
      ConsumerInstanceId instanceId,
      ConsumerConnector consumer
  ) {
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
  public ConsumerRecordAndSize<Object, Object> createConsumerRecord(
      MessageAndMetadata<byte[], byte[]> msg
  ) {
    long approxSize = 0;

    Object key = null;
    Object value = null;

    // The extra serialization here is unfortunate. We could use @JsonRawValue
    // and just use the raw bytes, but that risks returning invalid data to the user
    // if their data is not actually JSON encoded.

    if (msg.key() != null) {
      approxSize += msg.key().length;
      key = deserialize(msg.key());
    }

    if (msg.message() != null) {
      approxSize += msg.message().length;
      value = deserialize(msg.message());
    }

    return new ConsumerRecordAndSize<>(
        new JsonConsumerRecord(msg.topic(), key, value, msg.partition(), msg.offset()),
        approxSize
    );
  }

  private Object deserialize(byte[] data) {
    try {
      return objectMapper.readValue(data, Object.class);
    } catch (Exception e) {
      throw new SerializationException(e);
    }
  }
}
