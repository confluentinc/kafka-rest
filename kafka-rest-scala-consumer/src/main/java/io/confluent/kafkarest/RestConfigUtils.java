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

import static io.confluent.kafkarest.KafkaRestConfig.BOOTSTRAP_SERVERS_CONFIG;
import static io.confluent.kafkarest.KafkaRestConfig.KAFKACLIENT_ZK_SESSION_TIMEOUT_MS_CONFIG;
import static io.confluent.kafkarest.KafkaRestConfig.ZOOKEEPER_CONNECT_CONFIG;

import java.util.List;
import kafka.cluster.Broker;
import kafka.cluster.EndPoint;
import kafka.utils.ZkUtils;
import org.apache.kafka.common.security.JaasUtils;
import org.apache.kafka.common.utils.Utils;
import org.eclipse.jetty.util.StringUtil;
import scala.collection.JavaConversions;
import scala.collection.Seq;

public class RestConfigUtils {

  public static String bootstrapBrokers(KafkaRestConfig config) {
    int zkSessionTimeoutMs = config.getInt(KAFKACLIENT_ZK_SESSION_TIMEOUT_MS_CONFIG);

    String bootstrapServersConfig = config.getString(BOOTSTRAP_SERVERS_CONFIG);
    if (StringUtil.isNotBlank(bootstrapServersConfig)) {
      return bootstrapServersConfig;
    }
    ZkUtils zkUtils = null;
    try {
      zkUtils =
          ZkUtils.apply(
              config.getString(ZOOKEEPER_CONNECT_CONFIG),
              zkSessionTimeoutMs,
              zkSessionTimeoutMs,
              JaasUtils.isZkSecurityEnabled()
          );
      return getBootstrapBrokers(zkUtils);
    } finally {
      if (zkUtils != null) {
        zkUtils.close();
      }
    }
  }

  private static String getBootstrapBrokers(ZkUtils zkUtils) {
    Seq<Broker> brokerSeq = zkUtils.getAllBrokersInCluster();

    List<Broker> brokers = JavaConversions.seqAsJavaList(brokerSeq);
    String bootstrapBrokers = "";
    for (int i = 0; i < brokers.size(); i++) {
      for (EndPoint ep : JavaConversions.asJavaCollection(brokers.get(i).endPoints())) {
        if (bootstrapBrokers.length() > 0) {
          bootstrapBrokers += ",";
        }
        String hostport =
            ep.host() == null ? ":" + ep.port() : Utils.formatAddress(ep.host(), ep.port());
        bootstrapBrokers += ep.securityProtocol() + "://" + hostport;
      }
    }
    return bootstrapBrokers;
  }
}
