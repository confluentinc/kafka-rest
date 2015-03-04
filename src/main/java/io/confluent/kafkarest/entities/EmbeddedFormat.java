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

package io.confluent.kafkarest.entities;

import com.fasterxml.jackson.annotation.JsonFormat;
import io.confluent.kafkarest.converters.AvroConverter;
import kafka.message.MessageAndOffset;

import java.nio.ByteBuffer;

/**
 * Permitted formats for ProduceRecords embedded in produce requests/consume responses, e.g.
 * base64-encoded binary, JSON-encoded Avro, etc. Each of these correspond to a content type, a
 * ProduceRecord implementation, a Producer in the ProducerPool (with corresponding Kafka
 * serializer), ConsumerRecord implementation, and a serializer for any instantiated consumers.
 *
 * Note that for each type, it's assumed that the key and value can be handled by the same
 * serializer. This means each serializer should handle both it's complex type (e.g.
 * Indexed/Generic/SpecificRecord for Avro) and boxed primitive types (Integer, Boolean, etc.).
 */
@JsonFormat(shape = JsonFormat.Shape.STRING)
public enum EmbeddedFormat {
  BINARY {
    @Override
    public ConsumerRecord createConsumerRecord(final MessageAndOffset msg, final int partition) {
      byte[] keyBytes = null;
      byte[] payloadBytes = null;

      if (msg.message().key() != null) {
        final ByteBuffer key = msg.message().key();
        keyBytes = new byte[key.remaining()];
        key.get(keyBytes);
      }

      if (msg.message().payload() != null) {
        final ByteBuffer payload = msg.message().payload();
        payloadBytes = new byte[payload.remaining()];
        payload.get(payloadBytes);
      }

      return new BinaryConsumerRecord(keyBytes, payloadBytes, partition, msg.offset());
    }
  },
  AVRO {
    @Override
    public ConsumerRecord createConsumerRecord(final MessageAndOffset msg, final int partition) {
      AvroConverter.JsonNodeAndSize keyNode = AvroConverter.toJson(msg.message().key());
      AvroConverter.JsonNodeAndSize valueNode = AvroConverter.toJson(msg.message().payload());
      return new AvroConsumerRecord(keyNode.json, valueNode.json, partition, msg.offset());
    }
  };

  public abstract ConsumerRecord createConsumerRecord(final MessageAndOffset msg, final int partition);
}
