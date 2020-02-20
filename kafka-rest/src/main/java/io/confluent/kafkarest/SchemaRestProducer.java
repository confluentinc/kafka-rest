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

package io.confluent.kafkarest;

import com.fasterxml.jackson.databind.JsonNode;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.SchemaProvider;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDe;
import io.confluent.kafkarest.converters.ConversionException;
import io.confluent.kafkarest.converters.SchemaConverter;
import io.confluent.kafkarest.entities.ProduceRecord;
import io.confluent.kafkarest.entities.ProduceRequest;
import io.confluent.rest.exceptions.RestException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class SchemaRestProducer implements RestProducer<JsonNode, JsonNode> {

  protected final KafkaProducer<Object, Object> producer;
  protected final AbstractKafkaSchemaSerDe keySerializer;
  protected final AbstractKafkaSchemaSerDe valueSerializer;
  protected final SchemaProvider schemaProvider;
  protected final SchemaConverter schemaConverter;
  protected final Map<ParsedSchema, Integer> schemaIdCache;

  public SchemaRestProducer(
      KafkaProducer<Object, Object> producer,
      AbstractKafkaSchemaSerDe keySerializer,
      AbstractKafkaSchemaSerDe valueSerializer,
      SchemaProvider schemaProvider,
      SchemaConverter schemaConverter
  ) {
    this.producer = producer;
    this.keySerializer = keySerializer;
    this.valueSerializer = valueSerializer;
    this.schemaProvider = schemaProvider;
    this.schemaConverter = schemaConverter;
    this.schemaIdCache = new ConcurrentHashMap<>(100);
  }

  public void produce(
      ProduceTask task,
      String topic,
      Integer partition,
      Collection<? extends ProduceRecord<JsonNode, JsonNode>> records
  ) {
    ProduceRequest<?, ?> schemaHolder = task.getSchemaHolder();
    ParsedSchema keySchema = null;
    ParsedSchema valueSchema = null;
    Integer keySchemaId = schemaHolder.getKeySchemaId();
    Integer valueSchemaId = schemaHolder.getValueSchemaId();
    try {
      // If both ID and schema are null, that may be ok. Validation of the ProduceTask by the
      // caller should have checked this already.
      if (keySchemaId != null) {
        keySchema = keySerializer.getSchemaById(keySchemaId);
      } else if (schemaHolder.getKeySchema() != null) {
        keySchema = schemaProvider.parseSchema(
                schemaHolder.getKeySchema(), Collections.emptyList())
                .orElseThrow(() -> Errors.invalidSchemaException(schemaHolder.getKeySchema())
        );
        if (schemaIdCache.containsKey(keySchema)) {
          keySchemaId = schemaIdCache.get(keySchema);
          keySchema = keySerializer.getSchemaById(keySchemaId);
        } else {
          keySchemaId = keySerializer.register(topic + "-key", keySchema);
          schemaIdCache.put(keySchema, keySchemaId);
        }
      }

      if (valueSchemaId != null) {
        valueSchema = valueSerializer.getSchemaById(valueSchemaId);
      } else if (schemaHolder.getValueSchema() != null) {
        valueSchema = schemaProvider.parseSchema(
                schemaHolder.getValueSchema(), Collections.emptyList())
                .orElseThrow(() -> Errors.invalidSchemaException(schemaHolder.getValueSchema())
        );
        if (schemaIdCache.containsKey(valueSchema)) {
          valueSchemaId = schemaIdCache.get(valueSchema);
          valueSchema = valueSerializer.getSchemaById(valueSchemaId);
        } else {
          valueSchemaId = valueSerializer.register(topic + "-value", valueSchema);
          schemaIdCache.put(valueSchema, valueSchemaId);
        }
      }
    } catch (RestClientException e) {
      // FIXME We should return more specific error codes (unavailable vs registration failed in
      // a way that isn't retriable?).
      throw new RestException("Schema registration or lookup failed", 408, 40801, e);
    } catch (IOException e) {
      throw new RestException("Schema registration or lookup failed", 408, 40801, e);
    }

    // Store the schema IDs in the task. These will be used to include the IDs in the response
    task.setSchemaIds(keySchemaId, valueSchemaId);

    // Convert everything before doing any sends so if any conversion fails we can kill
    // the entire request so we don't get partially sent requests
    ArrayList<ProducerRecord<Object, Object>> kafkaRecords
        = new ArrayList<ProducerRecord<Object, Object>>();
    try {
      for (ProduceRecord<JsonNode, JsonNode> record : records) {
        // Beware of null schemas and NullNodes here: we need to avoid attempting the conversion
        // if there isn't a schema. Validation will have already checked that all the keys/values
        // were NullNodes.
        Object key = keySchema != null
            ? schemaConverter.toObject(record.getKey(), keySchema) : null;
        Object value = valueSchema != null
            ? schemaConverter.toObject(record.getValue(), valueSchema) : null;
        Integer recordPartition = partition;
        if (recordPartition == null) {
          recordPartition = record.getPartition();
        }
        kafkaRecords.add(new ProducerRecord(topic, recordPartition, key, value));
      }
    } catch (ConversionException e) {
      throw Errors.jsonConversionException(e);
    }
    for (ProducerRecord<Object, Object> rec : kafkaRecords) {
      producer.send(rec, task.createCallback());
    }
  }

  public void close() {
    producer.close();
  }
}
