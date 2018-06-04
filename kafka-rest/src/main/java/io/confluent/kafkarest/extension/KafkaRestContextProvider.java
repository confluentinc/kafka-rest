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

package io.confluent.kafkarest.extension;

import io.confluent.kafkarest.AdminClientWrapper;
import io.confluent.kafkarest.ClusterInformationObserver;
import io.confluent.kafkarest.ConsumerManager;
import io.confluent.kafkarest.DefaultKafkaRestContext;
import io.confluent.kafkarest.GroupMetadataObserver;
import io.confluent.kafkarest.KafkaRestConfig;
import io.confluent.kafkarest.KafkaRestContext;
import io.confluent.kafkarest.ProducerPool;
import io.confluent.kafkarest.ScalaConsumersContext;
import org.apache.kafka.common.security.JaasUtils;
import org.eclipse.jetty.util.StringUtil;

import java.util.concurrent.atomic.AtomicBoolean;
import io.confluent.kafkarest.v2.KafkaConsumerManager;

import java.util.concurrent.atomic.AtomicBoolean;
import javax.ws.rs.core.Configurable;

public class KafkaRestContextProvider {

  private static KafkaRestContext defaultContext = null;
  private static KafkaRestConfig defaultAppConfig = null;
  private static final InheritableThreadLocal<KafkaRestContext> restContextInheritableThreadLocal =
      new InheritableThreadLocal<>();

  private static AtomicBoolean initialized = new AtomicBoolean();

  public static void initialize(
      Configurable<?> config,
      KafkaRestConfig appConfig,
      ProducerPool producerPool,
      KafkaConsumerManager kafkaConsumerManager,
      AdminClientWrapper adminClientWrapper,
      ClusterInformationObserver clusterInformationObserver,
      GroupMetadataObserver groupMetadataObserver,
      ScalaConsumersContext scalaConsumersContext
  ) {
    if (initialized.compareAndSet(false, true)) {

      if (scalaConsumersContext == null) {
        scalaConsumersContext = new ScalaConsumersContext(appConfig);
        ScalaConsumersContext.registerExceptionMappers(config, appConfig);
      }
      if (simpleConsumerFactory == null) {
        simpleConsumerFactory = new SimpleConsumerFactory(appConfig);
      }
      if (simpleConsumerManager == null) {
        simpleConsumerManager =
            new SimpleConsumerManager(appConfig, mdObserver, simpleConsumerFactory);
      }
      if (clusterInformationObserver == null) {
        clusterInformationObserver = new ClusterInformationObserver(zkUtils);
      }
      if (groupMetadataObserver == null) {
        groupMetadataObserver = new GroupMetadataObserver(appConfig);
      }
      defaultZkUtils = zkUtils;
      defaultContext =
          new DefaultKafkaRestContext(appConfig, mdObserver, producerPool, consumerManager,
              simpleConsumerManager, kafkaConsumerManager, adminClientWrapper,
              clusterInformationObserver, groupMetadataObserver, scalaConsumersContext);
      defaultAppConfig = appConfig;
    }
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
    defaultContext.shutdown();
    defaultContext = null;
    initialized.set(false);
  }
}
