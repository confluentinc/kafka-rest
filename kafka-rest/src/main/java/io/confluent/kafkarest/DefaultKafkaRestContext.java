/*
 * Copyright 2021 Confluent Inc.
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

import static io.confluent.kafkarest.config.SchemaRegistryConfig.SCHEMA_PROVIDERS;
import static java.util.Objects.requireNonNull;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafkarest.config.SchemaRegistryConfig;
import io.confluent.kafkarest.v2.KafkaConsumerManager;
import java.net.URI;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Shared, global state for the REST proxy server, including configuration and connection pools.
 * ProducerPool, AdminClientWrapper and KafkaConsumerManager instances are initialized lazily if
 * required.
 */
public class DefaultKafkaRestContext implements KafkaRestContext {

  private static final Logger log = LoggerFactory.getLogger(DefaultKafkaRestContext.class);

  private final KafkaRestConfig config;
  private KafkaConsumerManager kafkaConsumerManager;

  private SchemaRegistryClient schemaRegistryClient;

  /** @deprecated Use {@link #DefaultKafkaRestContext(KafkaRestConfig)} instead. */
  @Deprecated
  public DefaultKafkaRestContext(
      KafkaRestConfig config,
      ProducerPool producerPool,
      KafkaConsumerManager kafkaConsumerManager) {
    this(config);
  }

  public DefaultKafkaRestContext(KafkaRestConfig config) {
    log.debug("Creating context with config: {}", config);
    this.config = requireNonNull(config);
  }

  @Override
  public KafkaRestConfig getConfig() {
    return config;
  }

  @Override
  public ProducerPool getProducerPool() {
    return new ProducerPool(getProducer());
  }

  @Override
  public synchronized KafkaConsumerManager getKafkaConsumerManager() {
    if (kafkaConsumerManager == null) {
      kafkaConsumerManager = new KafkaConsumerManager(config);
    }
    return kafkaConsumerManager;
  }

  @Override
  public Admin getAdmin() {
    return AdminClient.create(config.getAdminProperties());
  }

  @Override
  public Producer<byte[], byte[]> getProducer() {
    return new KafkaProducer<>(
        config.getProducerConfigs(), new ByteArraySerializer(), new ByteArraySerializer());
  }

  @Override
  public SchemaRegistryClient getSchemaRegistryClient() {
    if (!config.isSchemaRegistryEnabled()) {
      return null;
    }
    if (schemaRegistryClient == null) {
      SchemaRegistryConfig schemaRegistryConfig =
          new SchemaRegistryConfig(config.getSchemaRegistryConfigs());

      List<String> schemaRegistryUrls =
          schemaRegistryConfig.getSchemaRegistryUrls().stream()
              .map(URI::create)
              .map(Object::toString)
              .collect(Collectors.toList());
      schemaRegistryClient =
          new CachedSchemaRegistryClient(
              schemaRegistryUrls,
              schemaRegistryConfig.getMaxSchemasPerSubject(),
              SCHEMA_PROVIDERS,
              config.getSchemaRegistryConfigs(),
              schemaRegistryConfig.requestHeaders());
    }
    return schemaRegistryClient;
  }

  @Override
  public void shutdown() {
    log.debug("Shutting down");
    if (kafkaConsumerManager != null) {
      kafkaConsumerManager.shutdown();
    }
  }
}
