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

import com.fasterxml.jackson.databind.JsonNode;

import java.util.Properties;

import io.confluent.kafka.serializers.KafkaAvroDecoder;
import io.confluent.kafkarest.converters.AvroConverter;
import io.confluent.kafkarest.entities.AvroConsumerRecord;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import kafka.serializer.Decoder;
import kafka.utils.VerifiableProperties;

/**
 * Avro implementation of ConsumerState, which decodes into GenericRecords or primitive types.
 */
public class AvroConsumerState extends ConsumerState<Object, Object, JsonNode, JsonNode> {

  // Note that this could be a static variable and shared, but that causes tests to break in
  // subtle ways because it causes state to be shared across tests, but only for the consumer.
  private Decoder<Object> decoder = null;

  public AvroConsumerState(KafkaRestConfig config,
                           ConsumerInstanceId instanceId,
                           ConsumerConnector consumer) {
    super(config, instanceId, consumer);
    Properties props = new Properties();
    props.setProperty("schema.registry.url",
                      config.getString(KafkaRestConfig.SCHEMA_REGISTRY_URL_CONFIG));
    decoder = new KafkaAvroDecoder(new VerifiableProperties(props));
  }

  @Override
  protected Decoder<Object> getKeyDecoder() {
    return decoder;
  }

  @Override
  protected Decoder<Object> getValueDecoder() {
    return decoder;
  }

  @Override
  public ConsumerRecordAndSize<JsonNode, JsonNode> createConsumerRecord(
      MessageAndMetadata<Object, Object> msg) {
    AvroConverter.JsonNodeAndSize keyNode = AvroConverter.toJson(msg.key());
    AvroConverter.JsonNodeAndSize valueNode = AvroConverter.toJson(msg.message());
    return new ConsumerRecordAndSize<>(
            new AvroConsumerRecord(msg.topic(),
                                   keyNode.json,
                                   valueNode.json,
                                   msg.partition(),
                                   msg.offset()),
            keyNode.size + valueNode.size
    );
  }
}
