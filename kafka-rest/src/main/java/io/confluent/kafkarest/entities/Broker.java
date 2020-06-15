/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.kafkarest.entities;

import com.google.auto.value.AutoValue;
import javax.annotation.Nullable;
import org.apache.kafka.common.Node;

/**
 * A Kafka broker.
 */
@AutoValue
public abstract class Broker {

  Broker() {
  }

  public abstract String getClusterId();

  public abstract int getBrokerId();

  @Nullable
  public abstract String getHost();

  @Nullable
  public abstract Integer getPort();

  @Nullable
  public abstract String getRack();

  public static Broker create(
      String clusterId,
      int brokerId,
      @Nullable String host,
      @Nullable Integer port,
      @Nullable String rack) {
    return new AutoValue_Broker(clusterId, brokerId, host, port, rack);
  }

  public static Broker fromNode(String clusterId, Node node) {
    return create(
        clusterId,
        node.id(),
        !node.host().equals("") ? node.host() : null,
        node.port() != -1 ? node.port() : null,
        node.rack());
  }

  public Node toNode() {
    return new Node(
        getBrokerId(),
        getHost() != null ? getHost() : "",
        getPort() != null ? getPort() : -1,
        getRack());
  }
}
