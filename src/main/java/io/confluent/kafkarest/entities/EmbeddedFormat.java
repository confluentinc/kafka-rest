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
import io.confluent.kafkarest.AvroConsumerState;
import io.confluent.kafkarest.BinaryConsumerState;
import io.confluent.kafkarest.ConsumerInstanceId;
import io.confluent.kafkarest.ConsumerManager;
import io.confluent.kafkarest.ConsumerState;
import io.confluent.kafkarest.JsonConsumerState;
import io.confluent.kafkarest.KafkaRestConfig;
import io.confluent.rest.exceptions.RestServerErrorException;

import javax.ws.rs.core.Response;
import java.util.Properties;

/**
 * Permitted formats for ProduceRecords embedded in produce requests/consume responses, e.g.
 * base64-encoded binary, JSON-encoded Avro, etc. Each of these correspond to a content type, a
 * ProduceRecord implementation, a Producer in the ProducerPool (with corresponding Kafka
 * serializer), AbstractConsumerRecord implementation, and a serializer for any instantiated consumers.
 *
 * Note that for each type, it's assumed that the key and value can be handled by the same
 * serializer. This means each serializer should handle both it's complex type (e.g.
 * Indexed/Generic/SpecificRecord for Avro) and boxed primitive types (Integer, Boolean, etc.).
 */
@JsonFormat(shape = JsonFormat.Shape.STRING)
public enum EmbeddedFormat {
  BINARY,
  AVRO,
  JSON;

  public static ConsumerState createConsumerState(EmbeddedFormat embeddedFormat,
                                                  KafkaRestConfig config,
                                                  ConsumerInstanceId cId,
                                                  Properties consumerProperties,
                                                  ConsumerManager.ConsumerFactory consumerFactory) {
    switch (embeddedFormat) {
      case BINARY:
        return new BinaryConsumerState(config, cId, consumerProperties, consumerFactory);

      case AVRO:
        return new AvroConsumerState(config, cId, consumerProperties, consumerFactory);

      case JSON:
        return new JsonConsumerState(config, cId, consumerProperties, consumerFactory);

      default:
        throw new RestServerErrorException("Invalid embedded format for new consumer.",
          Response.Status.INTERNAL_SERVER_ERROR.getStatusCode());
    }
  }
}
