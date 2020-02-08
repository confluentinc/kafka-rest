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
import org.apache.kafka.clients.admin.AdminClient;

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
  private GroupMetadataObserver groupMetadataObserver;


  public DefaultKafkaRestContext(
      KafkaRestConfig config,
      ProducerPool producerPool,
      KafkaConsumerManager kafkaConsumerManager,
      AdminClientWrapper adminClientWrapper,
      GroupMetadataObserver groupMetadataObserver,
      ScalaConsumersContext scalaConsumersContext
  ) {

    this.config = config;
    this.producerPool = producerPool;
    this.kafkaConsumerManager = kafkaConsumerManager;
    this.adminClientWrapper = adminClientWrapper;
    this.groupMetadataObserver = groupMetadataObserver;
    this.scalaConsumersContext = scalaConsumersContext;
  }


  @Override
  public KafkaRestConfig getConfig() {
    return config;
  }

  @Override
  public synchronized ProducerPool getProducerPool() {
    if (producerPool == null) {
      producerPool = new ProducerPool(config);
    }
    return producerPool;
  }

  @Override
  public ScalaConsumersContext getScalaConsumersContext() {
    return scalaConsumersContext;
  }

  @Override
  public ConsumerManager getConsumerManager() {
    return scalaConsumersContext.getConsumerManager();
  }

  @Override
  public SimpleConsumerManager getSimpleConsumerManager() {
    return scalaConsumersContext.getSimpleConsumerManager();
  }

  @Override
  public synchronized KafkaConsumerManager getKafkaConsumerManager() {
    if (kafkaConsumerManager == null) {
      kafkaConsumerManager = new KafkaConsumerManager(config);
    }
    return kafkaConsumerManager;
  }

  @Override
  public synchronized AdminClientWrapper getAdminClientWrapper() {
    if (adminClientWrapper == null) {
      adminClientWrapper = new AdminClientWrapper(config,
          AdminClient.create(AdminClientWrapper.adminProperties(config)));
    }
    return adminClientWrapper;
  }

  @Override
  public synchronized GroupMetadataObserver getGroupMetadataObserver() {
    if (groupMetadataObserver == null) {
      groupMetadataObserver = new GroupMetadataObserver(config, getAdminClientWrapper());
    }
    return groupMetadataObserver;
  }

  @Override
  public void shutdown() {
    if (kafkaConsumerManager != null) {
      kafkaConsumerManager.shutdown();
    }
    if (producerPool != null) {
      producerPool.shutdown();
    }
    if (adminClientWrapper != null) {
      adminClientWrapper.shutdown();
    }
    if (scalaConsumersContext != null) {
      scalaConsumersContext.shutdown();
    }
  }
}
