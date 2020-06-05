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

import io.confluent.kafkarest.v2.KafkaConsumerManager;
import java.util.Properties;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;

/**
 * Shared, global state for the REST proxy server, including configuration and connection pools.
 * ProducerPool, AdminClientWrapper and KafkaConsumerManager instances are initialized lazily
 * if required.
 */
public class DefaultKafkaRestContext implements KafkaRestContext {

  private final KafkaRestConfig config;
  private ProducerPool producerPool;
  private KafkaConsumerManager kafkaConsumerManager;
  private Admin adminClient;

  public DefaultKafkaRestContext(
      KafkaRestConfig config,
      ProducerPool producerPool,
      KafkaConsumerManager kafkaConsumerManager
  ) {
    this.config = config;
    this.producerPool = producerPool;
    this.kafkaConsumerManager = kafkaConsumerManager;
  }


  @Override
  public KafkaRestConfig getConfig() {
    return config;
  }

  @Override
  public ProducerPool getProducerPool() {
    if (producerPool == null) {
      producerPool = new ProducerPool(config);
    }
    return producerPool;
  }

  @Override
  public KafkaConsumerManager getKafkaConsumerManager() {
    if (kafkaConsumerManager == null) {
      kafkaConsumerManager = new KafkaConsumerManager(config);
    }
    return kafkaConsumerManager;
  }

  @Override
  public Admin getAdmin() {
    if (adminClient == null) {
      adminClient = AdminClient.create(adminProperties(config));
    }
    return adminClient;
  }

  public static Properties adminProperties(KafkaRestConfig kafkaRestConfig) {
    Properties properties = new Properties();
    properties.putAll(kafkaRestConfig.getAdminProperties());
    properties.put(
        KafkaRestConfig.BOOTSTRAP_SERVERS_CONFIG,
        RestConfigUtils.bootstrapBrokers(kafkaRestConfig));
    return properties;
  }

  @Override
  public void shutdown() {
    if (kafkaConsumerManager != null) {
      kafkaConsumerManager.shutdown();
    }
    if (producerPool != null) {
      producerPool.shutdown();
    }
    if (adminClient != null) {
      adminClient.close();
    }
  }
}
