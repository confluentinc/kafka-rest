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

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafkarest.converters.AvroConverter;
import io.confluent.kafkarest.converters.JsonConverter;
import io.confluent.kafkarest.entities.AvroConsumerRecord;
import io.confluent.kafkarest.entities.BinaryConsumerRecord;
import io.confluent.kafkarest.entities.ConsumerRecord;
import io.confluent.kafkarest.entities.EmbeddedFormat;
import io.confluent.kafkarest.entities.JsonConsumerRecord;
import io.confluent.rest.exceptions.RestException;
import io.confluent.rest.exceptions.RestServerErrorException;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.Deserializer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.Response;
import java.util.*;

public class SimpleConsumerManager {

  private static final Logger log = LoggerFactory.getLogger(SimpleConsumerManager.class);

  private final MetadataObserver mdObserver;

  private final SimpleConsumerPool simpleConsumersPool;

  private final Deserializer<Object> avroDeserializer;

  public SimpleConsumerManager(final KafkaRestConfig config,
                               final MetadataObserver mdObserver,
                               final ConsumerFactory<byte[], byte[]> simpleConsumerFactory) {

    this.mdObserver = mdObserver;

    int maxPoolSize = config.getInt(KafkaRestConfig.SIMPLE_CONSUMER_MAX_POOL_SIZE_CONFIG);
    int poolInstanceAvailabilityTimeoutMs = config.getInt(KafkaRestConfig.SIMPLE_CONSUMER_POOL_TIMEOUT_MS_CONFIG);
    int maxPollTime = config.getInt(KafkaRestConfig.SIMPLE_CONSUMER_MAX_POLL_TIME_CONFIG);
    Time time = config.getTime();

    ConsumerFactory<byte[], byte[]> consumerFactory = simpleConsumerFactory;

    if (consumerFactory == null) {
        consumerFactory = new ConsumerFactory<byte[], byte[]>() {
        private Properties consumerProperties = new Properties();

        {
          consumerProperties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
              config.getString(KafkaRestConfig.BOOTSTRAP_SERVERS_CONFIG));
        }

        @Override
        public Consumer<byte[], byte[]> createConsumer() {
          return new KafkaConsumer<byte[], byte[]>(consumerProperties,
              new ByteArrayDeserializer(),
              new ByteArrayDeserializer());
        }
      };
    }

    simpleConsumersPool =
      new SimpleConsumerPool(maxPoolSize,
          poolInstanceAvailabilityTimeoutMs, maxPollTime, time, consumerFactory);

    // Load deserializers
    Map<String, String> props = new HashMap<>();
    props.put("schema.registry.url", config.getString(KafkaRestConfig.SCHEMA_REGISTRY_URL_CONFIG));
    avroDeserializer = new KafkaAvroDeserializer();
    avroDeserializer.configure(props, true);
  }


  public void consume(final String topicName,
                      final int partitionId,
                      long offset,
                      long count,
                      final EmbeddedFormat embeddedFormat,
                      final ConsumerManager.ReadCallback callback) {

    List<ConsumerRecord> records = null;
    RestException exception = null;

    if (!mdObserver.topicExists(topicName)) {
      exception = Errors.topicNotFoundException();
    } else
    if (!mdObserver.partitionExists(topicName, partitionId)) {
      exception = Errors.partitionNotFoundException();
    } else {
      try (SimpleConsumerPool.RecordsFetcher fetcher =
               simpleConsumersPool.getRecordsFetcher(new TopicPartition(topicName, partitionId))) {

        List<org.apache.kafka.clients.consumer.ConsumerRecord<byte[], byte[]>> fetched =
          fetcher.poll(offset, count);

        records = new ArrayList<>(fetched.size());

        for (org.apache.kafka.clients.consumer.ConsumerRecord record: fetched) {
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
      JsonConverter.deserializeJson(consumerRecord.key()),
      JsonConverter.deserializeJson(consumerRecord.value()),
        topicName, partitionId, consumerRecord.offset());
  }

  private ConsumerRecord createConsumerRecord(
    final org.apache.kafka.clients.consumer.ConsumerRecord consumerRecord,
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
