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
import org.apache.avro.SchemaParseException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafkarest.converters.AvroConverter;
import io.confluent.kafkarest.converters.ConversionException;
import io.confluent.kafkarest.entities.ProduceRecord;
import io.confluent.kafkarest.entities.SchemaHolder;
import io.confluent.rest.exceptions.RestException;

public class AvroRestProducer implements RestProducer<JsonNode, JsonNode> {

  protected final KafkaProducer<Object, Object> producer;
  protected final KafkaAvroSerializer keySerializer;
  protected final KafkaAvroSerializer valueSerializer;
  protected final Map<Schema, Integer> schemaIdCache;

  public AvroRestProducer(
      KafkaProducer<Object, Object> producer,
      KafkaAvroSerializer keySerializer,
      KafkaAvroSerializer valueSerializer
  ) {
    this.producer = producer;
    this.keySerializer = keySerializer;
    this.valueSerializer = valueSerializer;
    this.schemaIdCache = new ConcurrentHashMap<>(100);
  }

  public void produce(
      ProduceTask task,
      String topic,
      Integer partition,
      Collection<? extends ProduceRecord<JsonNode, JsonNode>> records
  ) {
    SchemaHolder schemaHolder = task.getSchemaHolder();
    Schema keySchema = null;
    Schema valueSchema = null;
    Integer keySchemaId = schemaHolder.getKeySchemaId();
    Integer valueSchemaId = schemaHolder.getValueSchemaId();
    try {
      // If both ID and schema are null, that may be ok. Validation of the ProduceTask by the
      // caller should have checked this already.
      if (keySchemaId != null) {
        keySchema = keySerializer.getById(keySchemaId);
      } else if (schemaHolder.getKeySchema() != null) {
        keySchema = new Schema.Parser().parse(schemaHolder.getKeySchema());
        if (schemaIdCache.containsKey(keySchema)) {
          keySchemaId = schemaIdCache.get(keySchema);
          keySchema = keySerializer.getById(keySchemaId);
        } else {
          keySchemaId = keySerializer.register(topic + "-key", keySchema);
          schemaIdCache.put(keySchema, keySchemaId);
        }
      }

      if (valueSchemaId != null) {
        valueSchema = valueSerializer.getById(valueSchemaId);
      } else if (schemaHolder.getValueSchema() != null) {
        valueSchema = new Schema.Parser().parse(schemaHolder.getValueSchema());
        if (schemaIdCache.containsKey(valueSchema)) {
          valueSchemaId = schemaIdCache.get(valueSchema);
          valueSchema = valueSerializer.getById(valueSchemaId);
        } else {
          valueSchemaId = valueSerializer.register(topic + "-value", valueSchema);
          schemaIdCache.put(valueSchema, valueSchemaId);
        }
      }
    } catch (RestClientException e) {
      // FIXME We should return more specific error codes (unavailable vs registration failed in
      // a way that isn't retriable?).
      throw new RestException("Schema registration or lookup failed", 408, 40801, e);
    } catch (SchemaParseException e) {
      throw Errors.invalidSchemaException(e);
    } catch (IOException e) {
      throw new RestException("Schema registration or lookup failed", 408, 40801, e);
    }

    // Store the schema IDs in the task. These will be used to include the IDs in the response
    task.setSchemaIds(keySchemaId, valueSchemaId);

    // Convert everything to Avro before doing any sends so if any conversion fails we can kill
    // the entire request so we don't get partially sent requests
    ArrayList<ProducerRecord<Object, Object>> kafkaRecords
        = new ArrayList<ProducerRecord<Object, Object>>();
    try {
      for (ProduceRecord<JsonNode, JsonNode> record : records) {
        // Beware of null schemas and NullNodes here: we need to avoid attempting the conversion
        // if there isn't a schema. Validation will have already checked that all the keys/values
        // were NullNodes.
        Object key = (keySchema != null ? AvroConverter.toAvro(record.getKey(), keySchema) : null);
        Object value =
            valueSchema != null ? AvroConverter.toAvro(record.getValue(), valueSchema) : null;
        Integer recordPartition = partition;
        if (recordPartition == null) {
          recordPartition = record.partition();
        }
        kafkaRecords.add(new ProducerRecord(topic, recordPartition, key, value));
      }
    } catch (ConversionException e) {
      throw Errors.jsonAvroConversionException(e);
    }
    for (ProducerRecord<Object, Object> rec : kafkaRecords) {
      producer.send(rec, task.createCallback());
    }
  }

  public void close() {
    producer.close();
  }
}
