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

package io.confluent.kafkarest.integration;

import io.confluent.kafkarest.*;
import io.confluent.kafkarest.v2.KafkaConsumerManager;
import io.confluent.rest.RestConfigException;

import javax.ws.rs.core.Configurable;

/**
 * Test version of KakfaRestApplication that allows for dependency injection so components and be
 * tweaked for tests.
 */
public class TestKafkaRestApplication extends KafkaRestApplication {

  ProducerPool producerPoolInjected;
  KafkaConsumerManager kafkaConsumerManagerInjected;
  AdminClientWrapper adminClientWrapperInjected;
  GroupMetadataObserver groupMetadataObserverInjected;
  ScalaConsumersContext scalaConsumersContextInjected;

  public TestKafkaRestApplication(KafkaRestConfig config, ProducerPool producerPool,
                                  ConsumerManager consumerManager,
                                  SimpleConsumerFactory simpleConsumerFactory, SimpleConsumerManager simpleConsumerManager)
      throws IllegalAccessException, InstantiationException, RestConfigException {
    this(config, producerPool, null, null, null, null);
  }

  public TestKafkaRestApplication(KafkaRestConfig config, ProducerPool producerPool,
                                  KafkaConsumerManager kafkaConsumerManager,
                                  AdminClientWrapper adminClientWrapper,
                                  GroupMetadataObserver groupMetadataObserver,
                                  ScalaConsumersContext scalaConsumersContext) {
    super(config);
    producerPoolInjected = producerPool;
    kafkaConsumerManagerInjected = kafkaConsumerManager;
    adminClientWrapperInjected = adminClientWrapper;
    groupMetadataObserverInjected = groupMetadataObserver;
    scalaConsumersContextInjected = scalaConsumersContext;
  }

  @Override
  public void setupResources(Configurable<?> config, KafkaRestConfig appConfig) {
    setupInjectedResources(config, appConfig,
                           producerPoolInjected,
        kafkaConsumerManagerInjected, adminClientWrapperInjected,
        groupMetadataObserverInjected, scalaConsumersContextInjected);
  }

}
