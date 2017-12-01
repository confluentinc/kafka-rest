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

import com.fasterxml.jackson.databind.JsonNode;
import io.confluent.kafkarest.ConsumerInstanceId;
import io.confluent.kafkarest.ConsumerRecordAndSize;
import io.confluent.kafkarest.KafkaRestConfig;
import io.confluent.kafkarest.converters.AvroConverter;
import io.confluent.kafkarest.entities.AvroConsumerRecord;
import kafka.serializer.Decoder;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Properties;

/**
 * Avro implementation of KafkaConsumerState, which decodes into GenericRecords or primitive types.
 */
public class AvroKafkaConsumerState extends KafkaConsumerState<Object, Object, JsonNode, JsonNode> {

  // Note that this could be a static variable and shared, but that causes tests to break in
  // subtle ways because it causes state to be shared across tests, but only for the consumer.
  private Decoder<Object> decoder = null;

  public AvroKafkaConsumerState(KafkaRestConfig config,
      ConsumerInstanceId instanceId,
      Consumer consumer) {
    super(config, instanceId, consumer);
    Properties props = new Properties();
    props.setProperty("schema.registry.url",
        config.getString(KafkaRestConfig.SCHEMA_REGISTRY_URL_CONFIG));
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
      ConsumerRecord<Object, Object> record) {
    AvroConverter.JsonNodeAndSize keyNode = AvroConverter.toJson(record.key());
    AvroConverter.JsonNodeAndSize valueNode = AvroConverter.toJson(record.value());
    return new ConsumerRecordAndSize<JsonNode, JsonNode>(
        new AvroConsumerRecord(record.topic(), keyNode.json, valueNode.json, record.partition(),
            record.offset()),
        keyNode.size + valueNode.size
    );
  }
}
