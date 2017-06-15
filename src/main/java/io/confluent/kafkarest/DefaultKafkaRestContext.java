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
 */
public class DefaultKafkaRestContext implements KafkaRestContext {

  private final KafkaRestConfig config;
  private final MetadataObserver metadataObserver;
  private final ProducerPool producerPool;
  private final ConsumerManager consumerManager;
  private final KafkaConsumerManager kafkaConsumerManager;
  private final SimpleConsumerManager simpleConsumerManager;

  public DefaultKafkaRestContext(
      KafkaRestConfig config,
      MetadataObserver metadataObserver,
      ProducerPool producerPool,
      ConsumerManager consumerManager,
      SimpleConsumerManager simpleConsumerManager
  ) {
    this(config, metadataObserver, producerPool, consumerManager, simpleConsumerManager, null);
  }

  public DefaultKafkaRestContext(
      KafkaRestConfig config,
      MetadataObserver metadataObserver,
      ProducerPool producerPool,
      ConsumerManager consumerManager,
      SimpleConsumerManager simpleConsumerManager,
      KafkaConsumerManager kafkaConsumerManager
  ) {

    this.config = config;
    this.metadataObserver = metadataObserver;
    this.producerPool = producerPool;
    this.consumerManager = consumerManager;
    this.simpleConsumerManager = simpleConsumerManager;
    this.kafkaConsumerManager = kafkaConsumerManager;
  }

  public KafkaRestConfig getConfig() {
    return config;
  }

  public MetadataObserver getMetadataObserver() {
    return metadataObserver;
  }

  public ProducerPool getProducerPool() {
    return producerPool;
  }

  public ConsumerManager getConsumerManager() {
    return consumerManager;
  }

  public SimpleConsumerManager getSimpleConsumerManager() {
    return simpleConsumerManager;
  }

  public KafkaConsumerManager getKafkaConsumerManager() {
    return kafkaConsumerManager;
  }
}
