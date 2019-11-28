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
import io.confluent.kafkarest.converters.AvroConverter;
import io.confluent.kafkarest.entities.AvroConsumerRecord;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Avro implementation of KafkaConsumerState, which decodes into GenericRecords or primitive types.
 */
public class AvroKafkaConsumerState extends KafkaConsumerState<Object, Object, JsonNode, JsonNode> {

  public AvroKafkaConsumerState(KafkaRestConfig config,
      ConsumerInstanceId instanceId,
      Consumer consumer) {
    super(config, instanceId, consumer);
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
