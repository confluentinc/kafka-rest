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

import com.fasterxml.jackson.databind.JsonNode;
import io.confluent.kafkarest.ConsumerInstanceId;
import io.confluent.kafkarest.ConsumerRecordAndSize;
import io.confluent.kafkarest.KafkaRestConfig;
import io.confluent.kafkarest.converters.SchemaConverter;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Schema-specific implementation of KafkaConsumerState, which decodes
 * into Objects or primitive types.
 */
public final class SchemaKafkaConsumerState
    extends KafkaConsumerState<Object, Object, JsonNode, JsonNode> {

  private final SchemaConverter schemaConverter;

  public SchemaKafkaConsumerState(KafkaRestConfig config,
      ConsumerInstanceId instanceId,
      Consumer consumer,
      SchemaConverter schemaConverter) {
    super(config, instanceId, consumer);
    this.schemaConverter = schemaConverter;
  }

  @Override
  public ConsumerRecordAndSize<JsonNode, JsonNode> createConsumerRecord(
      ConsumerRecord<Object, Object> record) {
    SchemaConverter.JsonNodeAndSize keyNode = schemaConverter.toJson(record.key());
    SchemaConverter.JsonNodeAndSize valueNode = schemaConverter.toJson(record.value());
    return new ConsumerRecordAndSize<>(
        io.confluent.kafkarest.entities.ConsumerRecord.create(
            record.topic(),
            keyNode.getJson(),
            valueNode.getJson(),
            record.partition(),
            record.offset()),
        keyNode.getSize() + valueNode.getSize());
  }
}
