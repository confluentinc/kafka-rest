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

import static io.confluent.kafkarest.controllers.Entities.checkEntityExists;
import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;

import io.confluent.kafkarest.common.KafkaFutures;
import io.confluent.kafkarest.entities.Reassignment;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.inject.Inject;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.PartitionReassignment;
import org.apache.kafka.common.TopicPartition;

final class ReassignmentManagerImpl implements ReassignmentManager {

  private final Admin adminClient;
  private final ClusterManager clusterManager;

  @Inject
  ReassignmentManagerImpl(Admin adminClient,
      ClusterManager clusterManager) {
    this.adminClient = requireNonNull(adminClient);
    this.clusterManager = requireNonNull(clusterManager);
  }

  @Override
  public CompletableFuture<List<Reassignment>> listReassignments(
      String clusterId) {
    return clusterManager.getCluster(clusterId)
        .thenApply(cluster -> checkEntityExists(cluster,
            "Cluster %s cannot be found.", clusterId))
        .thenCompose(
            cluster -> KafkaFutures
                .toCompletableFuture(adminClient.listPartitionReassignments().reassignments()))
        .thenApply(
            reassignments -> {
              if (reassignments == null) {
                return emptyList();
              }
              return reassignments.entrySet().stream()
                  .map(reassignment -> toReassignment(clusterId, reassignment))
                  .sorted(
                      Comparator.comparing(Reassignment::getTopicName)
                          .thenComparing(Reassignment::getPartitionId))
                  .collect(Collectors.toList());
            });
  }

  @Override
  public CompletableFuture<List<Reassignment>> searchReassignmentsByTopicName(
      String clusterId, String topicName) {
    return listReassignments(clusterId)
        .thenApply(reassignments -> reassignments.stream()
            .filter(reassignment -> reassignment.getTopicName().equals(topicName))
            .sorted(
                Comparator.comparing(Reassignment::getTopicName)
                    .thenComparing(Reassignment::getPartitionId))
            .collect(Collectors.toList()));
  }

  @Override
  public CompletableFuture<Optional<Reassignment>> getReassignment(
      String clusterId, String topicName, Integer partitionId) {
    return listReassignments(clusterId)
        .thenApply(reassignments -> reassignments.stream()
            .filter(reassignment -> reassignment.getTopicName().equals(topicName))
            .filter(reassignment -> reassignment.getPartitionId() == partitionId)
            .findAny());
  }

  private static Reassignment toReassignment(String clusterId,
      Map.Entry<TopicPartition, PartitionReassignment> reassignment) {
    return Reassignment.create(
        clusterId,
        reassignment.getKey().topic(),
        reassignment.getKey().partition(),
        reassignment.getValue().addingReplicas(),
        reassignment.getValue().removingReplicas());
  }
}
