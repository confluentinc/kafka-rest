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

import org.apache.avro.Schema;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafkarest.converters.AvroConverter;
import io.confluent.kafkarest.entities.ProduceRecord;
import io.confluent.kafkarest.entities.SchemaHolder;
import io.confluent.rest.exceptions.RestException;

public class AvroRestProducer implements RestProducer<JsonNode,JsonNode> {
  protected final KafkaProducer<Object,Object> producer;
  protected final KafkaAvroSerializer keySerializer;
  protected final KafkaAvroSerializer valueSerializer;

  public AvroRestProducer(KafkaProducer<Object,Object> producer,
                          KafkaAvroSerializer keySerializer,
                          KafkaAvroSerializer valueSerializer) {
    this.producer = producer;
    this.keySerializer = keySerializer;
    this.valueSerializer = valueSerializer;
  }

  public void produce(ProduceTask task, String topic, Integer partition,
                      Collection<? extends ProduceRecord<JsonNode,JsonNode>> records) {
    SchemaHolder schemaHolder = task.getSchemaHolder();
    Schema keySchema, valueSchema;
    try {
      if (schemaHolder.getKeySchemaId() != null) {
        keySchema = keySerializer.getByID(schemaHolder.getKeySchemaId());
      } else {
        keySchema = new Schema.Parser().parse(schemaHolder.getKeySchema());
        schemaHolder.setKeySchemaId(keySerializer.register(topic, keySchema));
      }

      if (schemaHolder.getValueSchemaId() != null) {
        valueSchema = valueSerializer.getByID(schemaHolder.getKeySchemaId());
      } else {
        valueSchema = new Schema.Parser().parse(schemaHolder.getValueSchema());
        schemaHolder.setValueSchemaId(valueSerializer.register(topic, valueSchema));
      }
    } catch (IOException e) {
      // FIXME We should return more specific error codes (unavailable vs registration failed in
      // a way that isn't retriable?).
      throw new RestException("Schema registration or lookup failed", 408, 40801, e);
    }

    // Convert everything to Avro before doing any sends so if any conversion fails we can kill
    // the entire request so we don't get partially sent requests
    ArrayList<ProducerRecord<Object,Object>> kafkaRecords
        = new ArrayList<ProducerRecord<Object,Object>>();
    for (ProduceRecord<JsonNode,JsonNode> record : records) {
      Object key = AvroConverter.toAvro(record.getKey(), keySchema);
      Object value = AvroConverter.toAvro(record.getValue(), valueSchema);
      kafkaRecords.add(new ProducerRecord(topic, partition, key, value));
    }
    for(ProducerRecord<Object,Object> rec : kafkaRecords) {
      producer.send(rec, task);
    }
  }

  public void close() {
    producer.close();
  }
}
