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

import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaJsonDeserializer;
import io.confluent.kafkarest.converters.AvroConverter;
import io.confluent.kafkarest.entities.AvroConsumerRecord;
import io.confluent.kafkarest.entities.BinaryConsumerRecord;
import io.confluent.kafkarest.entities.ConsumerRecord;
import io.confluent.kafkarest.entities.EmbeddedFormat;
import io.confluent.kafkarest.entities.JsonConsumerRecord;
import io.confluent.rest.exceptions.RestException;
import io.confluent.rest.exceptions.RestServerErrorException;
import jersey.repackaged.com.google.common.collect.Maps;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class SimpleConsumerManager {

  private static final Logger log = LoggerFactory.getLogger(SimpleConsumerManager.class);

  private final int maxPoolSize;
  private final int poolInstanceAvailabilityTimeoutMs;
  private final Time time;

  private final MetadataObserver mdObserver;
  private final SimpleConsumerFactory simpleConsumerFactory;

  // stores pull of KafkaConsumer objects
  private final SimpleConsumerPool simpleConsumersPool;

  private final SimpleConsumerRecordsCache cache;

  private final Deserializer<Object> avroDeserializer;
  private final ObjectMapper objectMapper;

  public SimpleConsumerManager(final KafkaRestConfig config,
                               final MetadataObserver mdObserver,
                               final SimpleConsumerFactory simpleConsumerFactory) {

    this.mdObserver = mdObserver;
    this.simpleConsumerFactory = simpleConsumerFactory;

    maxPoolSize = config.getInt(KafkaRestConfig.SIMPLE_CONSUMER_MAX_POOL_SIZE_CONFIG);
    poolInstanceAvailabilityTimeoutMs = config.getInt(KafkaRestConfig.SIMPLE_CONSUMER_POOL_TIMEOUT_MS_CONFIG);
    time = config.getTime();

    simpleConsumersPool =
      new SimpleConsumerPool(maxPoolSize, poolInstanceAvailabilityTimeoutMs, time, simpleConsumerFactory);
    cache =
      new SimpleConsumerRecordsCache(config);

    // Load deserializers
    Properties props = new Properties();
    props.setProperty("schema.registry.url", config.getString(KafkaRestConfig.SCHEMA_REGISTRY_URL_CONFIG));
    avroDeserializer = new KafkaAvroDeserializer();
    avroDeserializer.configure(Maps.fromProperties(props), true);
    objectMapper = new ObjectMapper();

  }


  public void consume(final String topicName,
                      final int partitionId,
                      long offset,
                      long count,
                      final EmbeddedFormat embeddedFormat,
                      final ConsumerManager.ReadCallback callback) {

    List<ConsumerRecord> records = new ArrayList<>();
    RestException exception = null;

    if (!mdObserver.topicExists(topicName)) {
      exception = Errors.topicNotFoundException();
    } else
    if (!mdObserver.partitionExists(topicName, partitionId)) {
      exception = Errors.partitionNotFoundException();
    } else {
      try (TPConsumerState consumer = simpleConsumersPool.get(topicName, partitionId)) {
        List<org.apache.kafka.clients.consumer.ConsumerRecord<byte[], byte[]>> fetched =
          cache.pollRecords(consumer.consumer(), topicName, partitionId, offset, count);

        for (org.apache.kafka.clients.consumer.ConsumerRecord<byte[], byte[]> record: fetched) {
          records.add(createConsumerRecord(record, record.topic(), record.partition(), embeddedFormat));
        }

      } catch (Throwable e) {
        if (e instanceof RestException) {
          exception = (RestException) e;
        } else {
          exception = Errors.kafkaErrorException(e);
        }
      }
    }

    callback.onCompletion(records, exception);
  }

  private BinaryConsumerRecord createBinaryConsumerRecord(
    final org.apache.kafka.clients.consumer.ConsumerRecord<byte[], byte[]> consumerRecord,
    final String topicName,
    final int partitionId) {

    // KafkaConsumer instances are created with ByteArrayDeserializer so
    // there is no reason to deserialize record again.
    return new BinaryConsumerRecord(consumerRecord.key(), consumerRecord.value(),
        topicName, partitionId, consumerRecord.offset());
  }

  private AvroConsumerRecord createAvroConsumerRecord(
    final org.apache.kafka.clients.consumer.ConsumerRecord<byte[], byte[]> consumerRecord,
    final String topicName,
    final int partitionId) {

    return new AvroConsumerRecord(
        AvroConverter.toJson(avroDeserializer.deserialize(topicName, consumerRecord.key())).json,
        AvroConverter.toJson(avroDeserializer.deserialize(topicName, consumerRecord.value())).json,
            topicName,  partitionId, consumerRecord.offset());
  }


  private JsonConsumerRecord createJsonConsumerRecord(
    final org.apache.kafka.clients.consumer.ConsumerRecord<byte[], byte[]> consumerRecord,
    final String topicName,
    final int partitionId) {

    return new JsonConsumerRecord(
      deserializeJson(consumerRecord.key()),
      deserializeJson(consumerRecord.value()),
        topicName, partitionId, consumerRecord.offset());
  }

  private Object deserializeJson(byte[] data) {
    try {
      return data == null ? null : objectMapper.readValue(data, Object.class);
    } catch (Exception e) {
      throw new SerializationException(e);
    }
  }

  private ConsumerRecord createConsumerRecord(
    final org.apache.kafka.clients.consumer.ConsumerRecord<byte[], byte[]> consumerRecord,
    final String topicName,
    final int partitionId,
    final EmbeddedFormat embeddedFormat) {

    switch (embeddedFormat) {
      case BINARY:
        return createBinaryConsumerRecord(consumerRecord, topicName, partitionId);

      case AVRO:
        return createAvroConsumerRecord(consumerRecord, topicName, partitionId);

      case JSON:
        return createJsonConsumerRecord(consumerRecord, topicName, partitionId);

      default:
        throw new RestServerErrorException("Invalid embedded format for new consumer.",
            Response.Status.INTERNAL_SERVER_ERROR.getStatusCode());
    }
  }

  public void shutdown() {
    simpleConsumersPool.shutdown();
  }

}
