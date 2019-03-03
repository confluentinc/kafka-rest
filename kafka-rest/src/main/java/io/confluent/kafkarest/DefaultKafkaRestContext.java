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

import io.confluent.kafkarest.v2.KafkaConsumerManager;

/**
 * Shared, global state for the REST proxy server, including configuration and connection pools.
 * ProducerPool, AdminClientWrapper and KafkaConsumerManager instances are initialized lazily
 * if required.
 */
public class DefaultKafkaRestContext implements KafkaRestContext {

  private final KafkaRestConfig config;
  private final ScalaConsumersContext scalaConsumersContext;
  private ProducerPool producerPool;
  private KafkaConsumerManager kafkaConsumerManager;
  private AdminClientWrapper adminClientWrapper;

  public DefaultKafkaRestContext(
      KafkaRestConfig config,
      ProducerPool producerPool,
      KafkaConsumerManager kafkaConsumerManager,
      AdminClientWrapper adminClientWrapper,
      ScalaConsumersContext scalaConsumersContext
  ) {

    this.config = config;
    this.producerPool = producerPool;
    this.kafkaConsumerManager = kafkaConsumerManager;
    this.adminClientWrapper = adminClientWrapper;
    this.scalaConsumersContext = scalaConsumersContext;

    /*&
    if (mdObserver == null) {
        mdObserver = new MetadataObserver(zkUtils);
      }

      if (consumerManager == null) {
        consumerManager = new ConsumerManager(appConfig, mdObserver);
      }
      if (simpleConsumerFactory == null) {
        simpleConsumerFactory = new SimpleConsumerFactory(appConfig);
      }
      if (simpleConsumerManager == null) {
        simpleConsumerManager =
            new SimpleConsumerManager(appConfig, mdObserver, simpleConsumerFactory);
      }
     */
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
  public AdminClientWrapper getAdminClientWrapper() {
    if (adminClientWrapper == null) {
      adminClientWrapper = new AdminClientWrapper(config);
    }
    return adminClientWrapper;
  }

  @Override
  public SimpleConsumerManager getSimpleConsumerManager() {
    return scalaConsumersContext.getSimpleConsumerManager();
  }

  @Override
  public ConsumerManager getConsumerManager() {
    return scalaConsumersContext.getConsumerManager();
  }

  @Override
  public void shutdown() {
    if (kafkaConsumerManager != null) {
      kafkaConsumerManager.shutdown();
    }
    if (producerPool != null) {
      producerPool.shutdown();
    }
    if (scalaConsumersContext != null) {
      scalaConsumersContext.shutdown();
    }
  }
}
