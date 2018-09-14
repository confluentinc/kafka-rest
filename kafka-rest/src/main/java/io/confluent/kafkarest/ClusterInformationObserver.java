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

package io.confluent.kafkarest;

import io.confluent.kafkarest.entities.BrokerEntity;
import io.confluent.kafkarest.entities.EndPointEntity;
import kafka.cluster.Broker;
import kafka.cluster.EndPoint;
import kafka.utils.ZkUtils;
import scala.Option;
import scala.collection.JavaConverters$;

import java.util.ArrayList;
import java.util.List;

/**
 * Get kafka cluster information
 */
public class ClusterInformationObserver {

  private final ZkUtils zkUtils;

  public ClusterInformationObserver(ZkUtils zkUtils) {
    this.zkUtils = zkUtils;
  }

  public BrokerEntity getCurrentClusterController() {
    Option<Broker> brokerInfoOpt = zkUtils.getBrokerInfo(zkUtils.getController());
    if (brokerInfoOpt.isDefined()) {
      Broker brokerInfo = brokerInfoOpt.get();
      List<EndPoint> kafkaEndPoints = JavaConverters$.MODULE$
              .seqAsJavaListConverter(brokerInfo.endPoints()).asJava();
      List<EndPointEntity> result = new ArrayList<>();
      for (EndPoint eachKafkaEndPoint : kafkaEndPoints) {
        result.add(new EndPointEntity(eachKafkaEndPoint.securityProtocol().name(),
                eachKafkaEndPoint.host(),
                eachKafkaEndPoint.port()));
      }
      return new BrokerEntity(brokerInfo.id(), result);
    } else {
      return BrokerEntity.empty();
    }
  }

}
