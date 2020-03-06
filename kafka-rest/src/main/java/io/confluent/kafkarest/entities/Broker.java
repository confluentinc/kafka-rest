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

import java.util.Objects;
import java.util.StringJoiner;
import javax.annotation.Nullable;
import org.apache.kafka.common.Node;

/**
 * A Kafka broker.
 */
public final class Broker {

  private final String clusterId;

  private final int brokerId;

  @Nullable
  private final String host;

  @Nullable
  private final Integer port;

  @Nullable
  private final String rack;

  public Broker(
      String clusterId,
      int brokerId,
      @Nullable String host,
      @Nullable Integer port,
      @Nullable String rack) {
    this.clusterId = clusterId;
    this.brokerId = brokerId;
    this.host = host;
    this.port = port;
    this.rack = rack;
  }

  public String getClusterId() {
    return clusterId;
  }

  public int getBrokerId() {
    return brokerId;
  }

  @Nullable
  public String getHost() {
    return host;
  }

  @Nullable
  public Integer getPort() {
    return port;
  }

  @Nullable
  public String getRack() {
    return rack;
  }

  public static Broker fromNode(String clusterId, Node node) {
    return new Broker(
        clusterId,
        node.id(),
        !node.host().equals("") ? node.host() : null,
        node.port() != -1 ? node.port() : null,
        node.rack());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Broker broker = (Broker) o;
    return Objects.equals(clusterId, broker.clusterId)
        && brokerId == broker.brokerId
        && Objects.equals(host, broker.host)
        && Objects.equals(port, broker.port)
        && Objects.equals(rack, broker.rack);
  }

  @Override
  public int hashCode() {
    return Objects.hash(clusterId, brokerId, host, port, rack);
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", Broker.class.getSimpleName() + "[", "]")
        .add("clusterId=" + clusterId)
        .add("brokerId=" + brokerId)
        .add("host='" + host + "'")
        .add("port=" + port)
        .add("rack='" + rack + "'")
        .toString();
  }
}
