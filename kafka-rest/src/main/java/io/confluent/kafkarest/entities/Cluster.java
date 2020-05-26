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

import static java.util.Collections.unmodifiableList;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.StringJoiner;
import javax.annotation.Nullable;

/**
 * A Kafka cluster.
 */
public final class Cluster {

  private final String clusterId;

  @Nullable
  private final Broker controller;

  private final List<Broker> brokers;

  public Cluster(String clusterId, @Nullable Broker controller, List<Broker> brokers) {
    this.clusterId = Objects.requireNonNull(clusterId);
    this.controller = controller;
    this.brokers = unmodifiableList(brokers);
  }

  public String getClusterId() {
    return clusterId;
  }

  @Nullable
  public Broker getController() {
    return controller;
  }

  public List<Broker> getBrokers() {
    return brokers;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static Cluster create(
      String clusterId, @Nullable Broker controller, List<Broker> brokers) {
    return builder()
        .setClusterId(clusterId)
        .setController(controller)
        .addAllBrokers(brokers)
        .build();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Cluster cluster = (Cluster) o;
    return clusterId.equals(cluster.clusterId)
        && Objects.equals(controller, cluster.controller)
        && brokers.equals(cluster.brokers);
  }

  @Override
  public int hashCode() {
    return Objects.hash(clusterId, controller, brokers);
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", Cluster.class.getSimpleName() + "[", "]")
        .add("clusterId='" + clusterId + "'")
        .add("controller=" + controller)
        .add("brokers=" + brokers)
        .toString();
  }

  public static final class Builder {

    @Nullable
    private String clusterId;

    @Nullable
    private Broker controller;

    private final List<Broker> brokers = new ArrayList<>();

    public Builder() {
    }

    @Nullable
    public String getClusterId() {
      return clusterId;
    }

    public Builder setClusterId(String clusterId) {
      this.clusterId = clusterId;
      return this;
    }

    public Builder setController(Broker controller) {
      this.controller = controller;
      return this;
    }

    public Builder addAllBrokers(Collection<Broker> brokers) {
      this.brokers.addAll(brokers);
      return this;
    }

    public Cluster build() {
      return new Cluster(clusterId, controller, brokers);
    }
  }
}
