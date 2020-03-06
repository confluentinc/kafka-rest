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

package io.confluent.kafkarest.controllers;

import io.confluent.kafkarest.entities.Broker;
import io.confluent.kafkarest.entities.Cluster;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import javax.inject.Inject;
import javax.ws.rs.NotFoundException;

final class BrokerManagerImpl implements BrokerManager {

  private final ClusterManager clusterManager;

  @Inject
  BrokerManagerImpl(ClusterManager clusterManager) {
    this.clusterManager = Objects.requireNonNull(clusterManager);
  }

  @Override
  public CompletableFuture<List<Broker>> listBrokers(String clusterId) {
    return clusterManager.getCluster(clusterId)
        .thenApply(cluster ->
            cluster.map(Cluster::getBrokers)
                .orElseThrow(
                    () -> new NotFoundException(
                        String.format("Cluster %s cannot be found.", clusterId))));
  }

  @Override
  public CompletableFuture<Optional<Broker>> getBroker(String clusterId, int brokerId) {
    return listBrokers(clusterId)
        .thenApply(brokers ->
            brokers.stream().filter(broker -> broker.getBrokerId() == brokerId).findAny());
  }
}
