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

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.Collections.unmodifiableList;

import io.confluent.kafkarest.entities.Broker;
import io.confluent.kafkarest.entities.Cluster;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.inject.Inject;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DescribeClusterOptions;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.common.KafkaFuture;

final class ClusterManagerImpl implements ClusterManager {

  private final Admin adminClient;

  @Inject
  ClusterManagerImpl(Admin adminClient) {
    this.adminClient = Objects.requireNonNull(adminClient);
  }

  @Override
  public CompletableFuture<List<Cluster>> listClusters() {
    DescribeClusterResult describeClusterResult =
        adminClient.describeCluster(
            new DescribeClusterOptions().includeAuthorizedOperations(false));

    return CompletableFuture.completedFuture(new Cluster.Builder())
        .thenCombine(
            toCompletableFuture(describeClusterResult.clusterId()),
            (clusterBuilder, clusterId) -> {
              if (clusterId == null) {
                // If clusterId is null (e.g. MetadataResponse version is < 2), we can't address the
                // cluster by clusterId, therefore we are not interested on it.
                return null;
              }
              return clusterBuilder.setClusterId(clusterId);
            })
        .thenCombine(
            toCompletableFuture(describeClusterResult.controller()),
            (clusterBuilder, controller) -> {
              if (clusterBuilder == null) {
                return null;
              }
              if (controller == null || controller.isEmpty()) {
                return clusterBuilder;
              }
              return clusterBuilder.setController(Broker.fromNode(controller));
            })
        .thenCombine(
            toCompletableFuture(describeClusterResult.nodes()),
            (clusterBuilder, nodes) -> {
              if (clusterBuilder == null) {
                return null;
              }
              if (nodes == null) {
                return clusterBuilder;
              }
              return clusterBuilder.addAllBrokers(
                  nodes.stream()
                      .filter(node -> node != null && !node.isEmpty())
                      .map(Broker::fromNode)
                      .collect(Collectors.toList()));
            })
        .thenApply(
            clusterBuilder -> {
              if (clusterBuilder == null) {
                return emptyList();
              }
              return unmodifiableList(singletonList(clusterBuilder.build()));
            });
  }

  @Override
  public CompletableFuture<Optional<Cluster>> getCluster(String clusterId) {
    Objects.requireNonNull(clusterId);

    return listClusters().thenApply(
        clusters ->
            clusters.stream()
                .filter(cluster -> cluster.getClusterId().equals(clusterId))
                .findAny());
  }

  private static <T> CompletableFuture<T> toCompletableFuture(KafkaFuture<T> kafkaFuture) {
    CompletableFuture<T> completableFuture = new CompletableFuture<>();
    kafkaFuture.whenComplete(
        (value, exception) -> {
          if (exception == null) {
            completableFuture.complete(value);
          } else {
            completableFuture.completeExceptionally(exception);
          }
        });
    return completableFuture;
  }
}
