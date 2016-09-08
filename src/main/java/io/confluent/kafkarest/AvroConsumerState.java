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

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDecoder;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafkarest.converters.AvroConverter;
import io.confluent.kafkarest.entities.AvroConsumerRecord;
import jersey.repackaged.com.google.common.collect.Maps;
import kafka.utils.VerifiableProperties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Deserializer;

/**
 * Avro implementation of ConsumerState, which decodes into GenericRecords or primitive types.
 */
public class AvroConsumerState extends ConsumerState<Object, Object, JsonNode, JsonNode> {

  // Note that this could be a static variable and shared, but that causes tests to break in
  // subtle ways because it causes state to be shared across tests, but only for the consumer.
  private Deserializer<Object> deserializer = null;

  public AvroConsumerState(KafkaRestConfig config, ConsumerInstanceId instanceId,
                           Properties consumerProperties,
                           ConsumerManager.ConsumerFactory consumerFactory) {
    super(config, instanceId, consumerProperties, consumerFactory);
  }

  private Deserializer<Object> initDeserializer() {
    if (deserializer == null) {
      Properties props = new Properties();
      props.setProperty("schema.registry.url",
        config.getString(KafkaRestConfig.SCHEMA_REGISTRY_URL_CONFIG));
      deserializer = new KafkaAvroDeserializer();
      deserializer.configure(Maps.fromProperties(props), true);
    }
    return deserializer;
  }

  @Override
  protected Deserializer<Object> getKeyDeserializer() {
    return initDeserializer();
  }

  @Override
  protected Deserializer<Object> getValueDeserializer() {
    return initDeserializer();
  }

  @Override
  public ConsumerRecordAndSize<JsonNode, JsonNode> convertConsumerRecord(
      ConsumerRecord<Object, Object> msg) {
    AvroConverter.JsonNodeAndSize keyNode = AvroConverter.toJson(msg.key());
    AvroConverter.JsonNodeAndSize valueNode = AvroConverter.toJson(msg.value());
    return new ConsumerRecordAndSize<JsonNode, JsonNode>(
        new AvroConsumerRecord(keyNode.json, valueNode.json, msg.topic(), msg.partition(), msg.offset()),
        keyNode.size + valueNode.size
    );
  }
}
