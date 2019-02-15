/**
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

package io.confluent.kafkarest.integration;

import io.confluent.kafkarest.*;
import io.confluent.kafkarest.v2.KafkaConsumerManager;
import io.confluent.rest.RestConfigException;
import kafka.utils.ZkUtils;

import javax.ws.rs.core.Configurable;

/**
 * Test version of KakfaRestApplication that allows for dependency injection so components and be
 * tweaked for tests.
 */
public class TestKafkaRestApplication extends KafkaRestApplication {

  ZkUtils zkUtilsInjected;
  MetadataObserver mdObserverInjected;
  ProducerPool producerPoolInjected;
  ConsumerManager consumerManagerInjected;
  SimpleConsumerFactory simpleConsumerFactoryInjected;
  SimpleConsumerManager simpleConsumerManagerInjected;
  KafkaConsumerManager kafkaConsumerManagerInjected;
  AdminClientWrapper adminClientWrapperInjected;

  public TestKafkaRestApplication(KafkaRestConfig config, ZkUtils zkUtils,
                                  MetadataObserver mdObserver, ProducerPool producerPool,
                                  ConsumerManager consumerManager,
                                  SimpleConsumerFactory simpleConsumerFactory, SimpleConsumerManager simpleConsumerManager)
      throws IllegalAccessException, InstantiationException, RestConfigException {
    this(config, zkUtils, mdObserver, producerPool, consumerManager,
        simpleConsumerFactory, simpleConsumerManager, null, null);
  }

  public TestKafkaRestApplication(KafkaRestConfig config, ZkUtils zkUtils,
                                  MetadataObserver mdObserver, ProducerPool producerPool,
                                  ConsumerManager consumerManager,
                                  SimpleConsumerFactory simpleConsumerFactory,
                                  SimpleConsumerManager simpleConsumerManager,
                                  KafkaConsumerManager kafkaConsumerManager, AdminClientWrapper adminClientWrapper)
      throws IllegalAccessException, InstantiationException, RestConfigException {
    super(config);
    zkUtilsInjected = zkUtils;
    mdObserverInjected = mdObserver;
    producerPoolInjected = producerPool;
    consumerManagerInjected = consumerManager;
    simpleConsumerFactoryInjected = simpleConsumerFactory;
    simpleConsumerManagerInjected = simpleConsumerManager;
    kafkaConsumerManagerInjected = kafkaConsumerManager;
    adminClientWrapperInjected = adminClientWrapper;
  }

  @Override
  public void setupResources(Configurable<?> config, KafkaRestConfig appConfig) {
    setupInjectedResources(config, appConfig, zkUtilsInjected, mdObserverInjected,
                           producerPoolInjected, consumerManagerInjected,
                           simpleConsumerFactoryInjected, simpleConsumerManagerInjected,
        kafkaConsumerManagerInjected, adminClientWrapperInjected);
  }

}
