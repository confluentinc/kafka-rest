/*
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

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaJsonSerializer;
import io.confluent.kafkarest.entities.EmbeddedFormat;
import io.confluent.kafkarest.entities.ProduceRecord;
import io.confluent.kafkarest.entities.SchemaHolder;

/**
 * Shared pool of Kafka producers used to send messages. The pool manages batched sends, tracking
 * all required acks for a batch and managing timeouts. Currently this pool only contains one
 * producer per serialization format (e.g. byte[], Avro).
 */
public class ProducerPool {

  private static final Logger log = LoggerFactory.getLogger(ProducerPool.class);
  private Map<EmbeddedFormat, RestProducer> producers =
      new HashMap<EmbeddedFormat, RestProducer>();

  public ProducerPool(KafkaRestConfig appConfig) {
    this(appConfig, null);
  }

  public ProducerPool(
      KafkaRestConfig appConfig,
      Properties producerConfigOverrides
  ) {
    this(appConfig, appConfig.bootstrapBrokers(), producerConfigOverrides);
  }

  public ProducerPool(
      KafkaRestConfig appConfig,
      String bootstrapBrokers,
      Properties producerConfigOverrides
  ) {

    Map<String, Object> binaryProps =
        buildStandardConfig(appConfig, bootstrapBrokers, producerConfigOverrides);
    producers.put(EmbeddedFormat.BINARY, buildBinaryProducer(binaryProps));

    Map<String, Object> jsonProps =
        buildStandardConfig(appConfig, bootstrapBrokers, producerConfigOverrides);
    producers.put(EmbeddedFormat.JSON, buildJsonProducer(jsonProps));

    Map<String, Object> avroProps =
        buildAvroConfig(appConfig, bootstrapBrokers, producerConfigOverrides);
    producers.put(EmbeddedFormat.AVRO, buildAvroProducer(avroProps));
  }

  private Map<String, Object> buildStandardConfig(
      KafkaRestConfig appConfig,
      String bootstrapBrokers,
      Properties producerConfigOverrides
  ) {
    Map<String, Object> props = new HashMap<String, Object>();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapBrokers);

    Properties producerProps = (Properties) appConfig.getProducerProperties();
    return buildConfig(props, producerProps, producerConfigOverrides);
  }

  private NoSchemaRestProducer<byte[], byte[]> buildBinaryProducer(
      Map<String, Object>
          binaryProps
  ) {
    return buildNoSchemaProducer(binaryProps, new ByteArraySerializer(), new ByteArraySerializer());
  }

  private NoSchemaRestProducer<Object, Object> buildJsonProducer(Map<String, Object> jsonProps) {
    return buildNoSchemaProducer(jsonProps, new KafkaJsonSerializer(), new KafkaJsonSerializer());
  }

  private <K, V> NoSchemaRestProducer<K, V> buildNoSchemaProducer(
      Map<String, Object> props,
      Serializer<K> keySerializer,
      Serializer<V> valueSerializer
  ) {
    keySerializer.configure(props, true);
    valueSerializer.configure(props, false);
    KafkaProducer<K, V> producer =
        new KafkaProducer<K, V>(props, keySerializer, valueSerializer);
    return new NoSchemaRestProducer<K, V>(producer);
  }

  private Map<String, Object> buildAvroConfig(
      KafkaRestConfig appConfig,
      String bootstrapBrokers,
      Properties producerConfigOverrides
  ) {
    Map<String, Object> avroDefaults = new HashMap<String, Object>();
    avroDefaults.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapBrokers);
    avroDefaults.put(
        "schema.registry.url",
        appConfig.getString(KafkaRestConfig.SCHEMA_REGISTRY_URL_CONFIG)
    );

    Properties producerProps = (Properties) appConfig.getProducerProperties();
    return buildConfig(avroDefaults, producerProps, producerConfigOverrides);
  }

  private AvroRestProducer buildAvroProducer(Map<String, Object> avroProps) {
    final KafkaAvroSerializer avroKeySerializer = new KafkaAvroSerializer();
    avroKeySerializer.configure(avroProps, true);
    final KafkaAvroSerializer avroValueSerializer = new KafkaAvroSerializer();
    avroValueSerializer.configure(avroProps, false);
    KafkaProducer<Object, Object> avroProducer
        = new KafkaProducer<Object, Object>(avroProps, avroKeySerializer, avroValueSerializer);
    return new AvroRestProducer(avroProducer, avroKeySerializer, avroValueSerializer);
  }

  private Map<String, Object> buildConfig(
      Map<String, Object> defaults,
      Properties userProps,
      Properties overrides
  ) {
    // Note careful ordering: built-in values we look up automatically first, then configs
    // specified by user with initial KafkaRestConfig, and finally explicit overrides passed to
    // this method (only used for tests)
    Map<String, Object> config = new HashMap<String, Object>(defaults);
    for (String propName : userProps.stringPropertyNames()) {
      config.put(propName, userProps.getProperty(propName));
    }
    if (overrides != null) {
      for (String propName : overrides.stringPropertyNames()) {
        config.put(propName, overrides.getProperty(propName));
      }
    }
    return config;
  }

  public <K, V> void produce(
      String topic,
      Integer partition,
      EmbeddedFormat recordFormat,
      SchemaHolder schemaHolder,
      Collection<? extends ProduceRecord<K, V>> records,
      ProduceRequestCallback callback
  ) {
    ProduceTask task = new ProduceTask(schemaHolder, records.size(), callback);
    log.trace("Starting produce task " + task.toString());
    RestProducer restProducer = producers.get(recordFormat);
    restProducer.produce(task, topic, partition, records);
  }

  public void shutdown() {
    for (RestProducer restProducer : producers.values()) {
      restProducer.close();
    }
  }

  public interface ProduceRequestCallback {

    /**
     * Invoked when all messages have either been recorded or received an error
     *
     * @param results list of responses, in the same order as the request. Each entry can be either
     *                a RecordAndMetadata for successful responses or an exception
     */
    public void onCompletion(
        Integer keySchemaId,
        Integer valueSchemaId,
        List<RecordMetadataOrException> results
    );
  }
}
