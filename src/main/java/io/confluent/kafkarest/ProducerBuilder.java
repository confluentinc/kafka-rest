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

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaJsonSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serializer;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Build Kafka producer (binary, avro, no schema) from configuration.
 */
class ProducerBuilder {

  static Map<String, Object> buildStandardConfig(
      KafkaRestConfig appConfig,
      String bootstrapBrokers,
      Properties producerConfigOverrides
  ) {
    Map<String, Object> props = new HashMap<>();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapBrokers);

    Properties producerProps = appConfig.getProducerProperties();
    return buildConfig(props, producerProps, producerConfigOverrides);
  }

  static NoSchemaRestProducer<byte[], byte[]> buildBinaryProducer(
      Map<String, Object> binaryProps
  ) {
    return buildNoSchemaProducer(binaryProps, new ByteArraySerializer(), new ByteArraySerializer());
  }

  static NoSchemaRestProducer<Object, Object> buildJsonProducer(Map<String, Object> jsonProps) {
    return buildNoSchemaProducer(jsonProps,
                                 new KafkaJsonSerializer<>(),
                                 new KafkaJsonSerializer<>());
  }

  static Map<String, Object> buildAvroConfig(
      KafkaRestConfig appConfig,
      String bootstrapBrokers,
      Properties producerConfigOverrides
  ) {
    Map<String, Object> avroDefaults = new HashMap<>();
    avroDefaults.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapBrokers);
    avroDefaults.put(
        "schema.registry.url",
        appConfig.getString(KafkaRestConfig.SCHEMA_REGISTRY_URL_CONFIG)
    );

    Properties producerProps = appConfig.getProducerProperties();
    return buildConfig(avroDefaults, producerProps, producerConfigOverrides);
  }

  static AvroRestProducer buildAvroProducer(Map<String, Object> avroProps) {
    final KafkaAvroSerializer avroKeySerializer = new KafkaAvroSerializer();
    avroKeySerializer.configure(avroProps, true);
    final KafkaAvroSerializer avroValueSerializer = new KafkaAvroSerializer();
    avroValueSerializer.configure(avroProps, false);
    KafkaProducer<Object, Object> avroProducer =
        new KafkaProducer<>(avroProps, avroKeySerializer, avroValueSerializer);
    return new AvroRestProducer(avroProducer, avroKeySerializer, avroValueSerializer);
  }

  private static <K, V> NoSchemaRestProducer<K, V> buildNoSchemaProducer(
      Map<String, Object> props,
      Serializer<K> keySerializer,
      Serializer<V> valueSerializer
  ) {
    keySerializer.configure(props, true);
    valueSerializer.configure(props, false);
    KafkaProducer<K, V> producer = new KafkaProducer<>(props, keySerializer, valueSerializer);
    return new NoSchemaRestProducer<>(producer);
  }

  private static Map<String, Object> buildConfig(
      Map<String, Object> defaults,
      Properties userProps,
      Properties overrides
  ) {
    // Note careful ordering: built-in values we look up automatically first, then configs
    // specified by user with initial KafkaRestConfig, and finally explicit overrides passed to
    // this method (only used for tests)
    Map<String, Object> config = new HashMap<>(defaults);
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
}
