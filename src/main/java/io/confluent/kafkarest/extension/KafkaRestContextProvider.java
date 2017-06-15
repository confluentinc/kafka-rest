/*
 * Copyright 2017 Confluent Inc.
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
 */

package io.confluent.kafkarest.extension;

import org.apache.kafka.common.security.JaasUtils;

import java.util.concurrent.atomic.AtomicBoolean;

import io.confluent.kafkarest.ConsumerManager;
import io.confluent.kafkarest.KafkaRestConfig;
import io.confluent.kafkarest.KafkaRestContext;
import io.confluent.kafkarest.DefaultKafkaRestContext;
import io.confluent.kafkarest.MetadataObserver;
import io.confluent.kafkarest.ProducerPool;
import io.confluent.kafkarest.SimpleConsumerFactory;
import io.confluent.kafkarest.SimpleConsumerManager;
import io.confluent.kafkarest.v2.KafkaConsumerManager;
import kafka.utils.ZkUtils;

public class KafkaRestContextProvider {

  private static KafkaRestContext defaultContext = null;
  private static ZkUtils defaultZkUtils = null;
  private static KafkaRestConfig defaultAppConfig = null;
  private static final InheritableThreadLocal<KafkaRestContext> restContextInheritableThreadLocal =
      new InheritableThreadLocal<>();

  private static AtomicBoolean initialized = new AtomicBoolean();

  public static void initialize(
      ZkUtils zkUtils,
      KafkaRestConfig appConfig,
      MetadataObserver mdObserver,
      ProducerPool producerPool,
      ConsumerManager consumerManager,
      SimpleConsumerFactory simpleConsumerFactory,
      SimpleConsumerManager simpleConsumerManager,
      KafkaConsumerManager kafkaConsumerManager
  ) {
    if (initialized.compareAndSet(false, true)) {
      if (zkUtils == null) {
        zkUtils = ZkUtils.apply(
            appConfig.getString(KafkaRestConfig.ZOOKEEPER_CONNECT_CONFIG), 30000,
            30000,
            JaasUtils.isZkSecurityEnabled()
        );
      }
      if (mdObserver == null) {
        mdObserver = new MetadataObserver(appConfig, zkUtils);
      }
      if (producerPool == null) {
        producerPool = new ProducerPool(appConfig, zkUtils);
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
      if (kafkaConsumerManager == null) {
        kafkaConsumerManager = new KafkaConsumerManager(appConfig);
      }

      defaultZkUtils = zkUtils;
      defaultContext =
          new DefaultKafkaRestContext(appConfig, mdObserver, producerPool, consumerManager,
              simpleConsumerManager, kafkaConsumerManager
          );
      defaultAppConfig = appConfig;
    }
  }

  public static ZkUtils getDefaultZkUtils() {
    return defaultZkUtils;
  }

  public static KafkaRestConfig getDefaultAppConfig() {
    return defaultAppConfig;
  }

  public static KafkaRestContext getDefaultContext() {
    return defaultContext;
  }

  public static KafkaRestContext getCurrentContext() {
    if (restContextInheritableThreadLocal.get() != null) {
      return restContextInheritableThreadLocal.get();
    } else {
      return defaultContext;
    }
  }

  public static void setCurrentContext(KafkaRestContext kafkaRestContext) {
    restContextInheritableThreadLocal.set(kafkaRestContext);
  }

  public static void clearCurrentContext() {
    restContextInheritableThreadLocal.remove();
  }

  public static synchronized void clean() {
    defaultContext.getConsumerManager().shutdown();
    defaultContext.getProducerPool().shutdown();
    defaultContext.getSimpleConsumerManager().shutdown();
    defaultContext.getMetadataObserver().shutdown();
    defaultContext = null;
    defaultZkUtils.close();
    initialized.set(false);
  }
}
