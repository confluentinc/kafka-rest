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

import kafka.zk.KafkaZkClient;
import org.apache.kafka.common.security.JaasUtils;
import org.apache.kafka.common.utils.SystemTime;
import org.eclipse.jetty.util.StringUtil;

public class ScalaConsumersContext {

  private final MetadataObserver metadataObserver;
  private final ConsumerManager consumerManager;
  private final SimpleConsumerManager simpleConsumerManager;

  public ScalaConsumersContext(final KafkaRestConfig config) {
    SimpleConsumerFactory simpleConsumerFactory = new SimpleConsumerFactory(config);
    metadataObserver = metadataObserver(config);
    consumerManager = new ConsumerManager(config, metadataObserver);
    simpleConsumerManager = new SimpleConsumerManager(config, metadataObserver,
        simpleConsumerFactory);
  }

  private MetadataObserver metadataObserver(final KafkaRestConfig config) {
    if (StringUtil.isNotBlank(config.getString(KafkaRestConfig.ZOOKEEPER_CONNECT_CONFIG))) {
      return new MetadataObserver(KafkaZkClient.apply(
              config.getString(KafkaRestConfig.ZOOKEEPER_CONNECT_CONFIG),
              JaasUtils.isZkSecurityEnabled(),30000, 30000, Integer.MAX_VALUE,
              new SystemTime(), "kafka.server", "SessionExpireListener"));
    } else {
      return new UnsupportedMetaDataObserver();
    }
  }

  public ScalaConsumersContext(MetadataObserver metadataObserver, ConsumerManager consumerManager,
                               SimpleConsumerManager simpleConsumerManager) {
    this.metadataObserver = metadataObserver;
    this.consumerManager = consumerManager;
    this.simpleConsumerManager = simpleConsumerManager;
  }

  public void shutdown() {
    metadataObserver.shutdown();
    consumerManager.shutdown();
    simpleConsumerManager.shutdown();
  }

  public SimpleConsumerManager getSimpleConsumerManager() {
    return simpleConsumerManager;
  }

  public ConsumerManager getConsumerManager() {
    return consumerManager;
  }

}
