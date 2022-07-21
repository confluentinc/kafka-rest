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

import static io.confluent.kafkarest.controllers.Entities.findEntityByKey;
import static java.util.Collections.singletonList;
import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;

import io.confluent.kafkarest.common.KafkaFutures;
import io.confluent.kafkarest.entities.Broker;
import io.confluent.kafkarest.entities.Cluster;
import io.confluent.kafkarest.exceptions.UnsupportedProtocolException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.inject.Inject;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DescribeClusterOptions;
import org.apache.kafka.clients.admin.DescribeClusterResult;

final class ClusterManagerImpl implements ClusterManager {

  private final Admin adminClient;

  @Inject
  ClusterManagerImpl(Admin adminClient) {
    this.adminClient = requireNonNull(adminClient);
  }

  @Override
  public CompletableFuture<List<Cluster>> listClusters() {
    return getLocalCluster().thenApply(cluster -> unmodifiableList(singletonList(cluster)));
  }

  @Override
  public CompletableFuture<Optional<Cluster>> getCluster(String clusterId) {
    requireNonNull(clusterId);
    return listClusters()
        .thenApply(clusters -> findEntityByKey(clusters, Cluster::getClusterId, clusterId));
  }

  @Override
  public CompletableFuture<Cluster> getLocalCluster() {
    DescribeClusterResult describeClusterResult =
        adminClient.describeCluster(
            new DescribeClusterOptions().includeAuthorizedOperations(false));

    return CompletableFuture.completedFuture(Cluster.builder())
        .thenCombine(
            KafkaFutures.toCompletableFuture(describeClusterResult.clusterId()),
            (clusterBuilder, clusterId) -> {
              if (clusterId == null) {
                // If clusterId is null (e.g. MetadataResponse version is < 2), we can't address the
                // cluster by clusterId, therefore we are not interested on it.
                throw new UnsupportedProtocolException(
                    "Metadata Response protocol version >= 2 required.");
              }
              return clusterBuilder.setClusterId(clusterId);
            })
        .thenCombine(
            KafkaFutures.toCompletableFuture(describeClusterResult.controller()),
            (clusterBuilder, controller) -> {
              if (controller == null || controller.isEmpty()) {
                return clusterBuilder;
              }
              return clusterBuilder.setController(
                  Broker.fromNode(clusterBuilder.build().getClusterId(), controller));
            })
        .thenCombine(
            KafkaFutures.toCompletableFuture(describeClusterResult.nodes()),
            (clusterBuilder, nodes) -> {
              if (nodes == null) {
                return clusterBuilder;
              }
              return clusterBuilder.addAllBrokers(
                  nodes.stream()
                      .filter(node -> node != null && !node.isEmpty())
                      .map(node -> Broker.fromNode(clusterBuilder.build().getClusterId(), node))
                      .collect(Collectors.toList()));
            })
        .thenApply(Cluster.Builder::build);
  }
}
