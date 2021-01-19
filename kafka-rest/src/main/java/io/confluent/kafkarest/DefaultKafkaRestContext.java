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

import static java.util.Objects.requireNonNull;

import io.confluent.kafkarest.v2.KafkaConsumerManager;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

/**
 * Shared, global state for the REST proxy server, including configuration and connection pools.
 * ProducerPool, AdminClientWrapper and KafkaConsumerManager instances are initialized lazily
 * if required.
 */
public class DefaultKafkaRestContext implements KafkaRestContext {

  private final KafkaRestConfig config;
  private KafkaConsumerManager kafkaConsumerManager;

  private Admin adminClient;
  private Producer<byte[], byte[]> producer;

  /**
   * @deprecated Use {@link #DefaultKafkaRestContext(KafkaRestConfig)} instead.
   */
  @Deprecated
  public DefaultKafkaRestContext(
      KafkaRestConfig config,
      ProducerPool producerPool,
      KafkaConsumerManager kafkaConsumerManager
  ) {
    this(config);
  }

  public DefaultKafkaRestContext(KafkaRestConfig config) {
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
  public synchronized Admin getAdmin() {
    if (adminClient == null) {
      adminClient = AdminClient.create(config.getAdminProperties());
    }
    return adminClient;
  }

  @Override
  public synchronized Producer<byte[], byte[]> getProducer() {
    if (producer == null) {
      producer =
          new KafkaProducer<>(
              config.getProducerConfigs(), new ByteArraySerializer(), new ByteArraySerializer());
    }
    return producer;
  }

  @Override
  public void shutdown() {
    if (kafkaConsumerManager != null) {
      kafkaConsumerManager.shutdown();
    }
    if (adminClient != null) {
      adminClient.close();
    }
    if (producer != null) {
      producer.close();
    }
  }
}
