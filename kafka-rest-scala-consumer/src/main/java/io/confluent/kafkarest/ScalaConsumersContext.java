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

import kafka.utils.ZkUtils;
import org.apache.kafka.common.security.JaasUtils;
import org.eclipse.jetty.util.StringUtil;

import static io.confluent.kafkarest.SimpleConsumerConfig.KAFKACLIENT_ZK_SESSION_TIMEOUT_MS_CONFIG;
import static io.confluent.kafkarest.SimpleConsumerConfig.ZOOKEEPER_CONNECT_CONFIG;

public class ScalaConsumersContext {

  private final MetadataObserver metadataObserver;
  private final ConsumerManager consumerManager;
  private final SimpleConsumerManager simpleConsumerManager;
  private ZkUtils zkUtils;

  public ScalaConsumersContext(final SimpleConsumerConfig config) {
    SimpleConsumerFactory simpleConsumerFactory = new SimpleConsumerFactory(config);
    metadataObserver = metadataObserver(config);
    consumerManager = new ConsumerManager(config, metadataObserver);
    simpleConsumerManager = new SimpleConsumerManager(config, metadataObserver,
        simpleConsumerFactory);
  }

  private MetadataObserver metadataObserver(final SimpleConsumerConfig config) {
    if (StringUtil.isNotBlank(config.getString(ZOOKEEPER_CONNECT_CONFIG))) {
      zkUtils = ZkUtils.apply(config.getString(ZOOKEEPER_CONNECT_CONFIG),
          config.getInt(KAFKACLIENT_ZK_SESSION_TIMEOUT_MS_CONFIG),
          config.getInt(KAFKACLIENT_ZK_SESSION_TIMEOUT_MS_CONFIG),
          JaasUtils.isZkSecurityEnabled());
      return new MetadataObserver(zkUtils);
    } else {
      return new UnsupportedMetaDataObserver(null);
    }
  }

  public ScalaConsumersContext(MetadataObserver metadataObserver, ConsumerManager consumerManager,
                               SimpleConsumerManager simpleConsumerManager) {
    this.metadataObserver = metadataObserver;
    this.consumerManager = consumerManager;
    this.simpleConsumerManager = simpleConsumerManager;
    this.zkUtils = null;
  }

  public void shutdown() {
    metadataObserver.shutdown();
    consumerManager.shutdown();
    simpleConsumerManager.shutdown();
    if (zkUtils != null) {
      zkUtils.close();
    }
  }

  public SimpleConsumerManager getSimpleConsumerManager() {
    return simpleConsumerManager;
  }

  public ConsumerManager getConsumerManager() {
    return consumerManager;
  }

}
