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

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serializer;

/**
 * Shared pool of Kafka producers used to send messages. The pool manages batched sends, tracking
 * all required acks for a batch and managing timeouts. Currently this pool only contains one
 * producer per serialization format (e.g. byte[], Avro).
 */
public class ProducerPool {

  private final KafkaProducer<byte[], byte[]> producer;

  public ProducerPool(KafkaRestConfig appConfig) {
    this(appConfig, null);
  }

  public ProducerPool(KafkaRestConfig appConfig, Properties producerConfigOverrides) {
    Map<String, Object> binaryProps = buildStandardConfig(appConfig, producerConfigOverrides);
    producer = buildBinaryProducer(binaryProps);
  }

  public Producer<byte[], byte[]> getProducer() {
    return producer;
  }

  private static Map<String, Object> buildStandardConfig(
      KafkaRestConfig appConfig, Properties producerConfigOverrides) {
    Map<String, Object> props = new HashMap<String, Object>();
    Properties producerProps = appConfig.getProducerProperties();
    return buildConfig(props, producerProps, producerConfigOverrides);
  }

  private static KafkaProducer<byte[], byte[]> buildBinaryProducer(
      Map<String, Object> binaryProps) {
    return buildNoSchemaProducer(binaryProps, new ByteArraySerializer(), new ByteArraySerializer());
  }

  private static KafkaProducer<byte[], byte[]> buildNoSchemaProducer(
      Map<String, Object> props,
      Serializer<byte[]> keySerializer,
      Serializer<byte[]> valueSerializer
  ) {
    keySerializer.configure(props, true);
    valueSerializer.configure(props, false);
    return new KafkaProducer<>(props, keySerializer, valueSerializer);
  }

  private static Map<String, Object> buildConfig(
      Map<String, Object> defaults, Properties userProps, Properties overrides) {
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

  public void shutdown() {
    producer.close();
  }
}
