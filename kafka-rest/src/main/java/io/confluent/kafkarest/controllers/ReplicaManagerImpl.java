/*
 * Copyright 2020 - 2022 Confluent Inc.
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
import static io.confluent.kafkarest.controllers.Entities.findEntityByKey;
import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableList;
import io.confluent.kafkarest.common.CompletableFutures;
import io.confluent.kafkarest.common.KafkaFutures;
import io.confluent.kafkarest.entities.Partition;
import io.confluent.kafkarest.entities.PartitionReplica;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DescribeLogDirsOptions;
import org.apache.kafka.clients.admin.DescribeLogDirsResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class ReplicaManagerImpl implements ReplicaManager {

  private final Admin adminClient;
  private final BrokerManager brokerManager;
  private final PartitionManager partitionManager;

  private static final Logger log = LoggerFactory.getLogger(ReplicaManagerImpl.class);

  @Inject
  ReplicaManagerImpl(
      Admin adminClient, BrokerManager brokerManager, PartitionManager partitionManager) {
    this.adminClient = requireNonNull(adminClient);
    this.brokerManager = requireNonNull(brokerManager);
    this.partitionManager = requireNonNull(partitionManager);
  }

  @Override
  public CompletableFuture<List<PartitionReplica>> listReplicas(
      String clusterId, String topicName, int partitionId) {
    return partitionManager
        .getPartition(clusterId, topicName, partitionId)
        .thenApply(
            partition ->
                checkEntityExists(
                    partition,
                    "Partition %d of topic %s could not be found on cluster %s.",
                    partitionId,
                    topicName,
                    clusterId))
        .thenApply(Partition::getReplicas);
  }

  @Override
  public CompletableFuture<Optional<PartitionReplica>> getReplica(
      String clusterId, String topicName, int partitionId, int brokerId) {
    return listReplicas(clusterId, topicName, partitionId)
        .thenApply(replicas -> findEntityByKey(replicas, PartitionReplica::getBrokerId, brokerId));
  }

  // Gets a partition-replica but returns an empty Optional if the entities do not exist
  CompletableFuture<Optional<PartitionReplica>> getReplicaAllowMissing(
      String clusterId, String topicName, int partitionId, int brokerId) {
    return partitionManager
        .getPartitionAllowMissing(clusterId, topicName, partitionId)
        .thenApply(partition -> partition.map(Partition::getReplicas).orElse(ImmutableList.of()))
        .thenApply(replicas -> findEntityByKey(replicas, PartitionReplica::getBrokerId, brokerId));
  }

  @Override
  public CompletableFuture<List<PartitionReplica>> searchReplicasByBrokerId(
      String clusterId, int brokerId) {
    return brokerManager
        .getBroker(clusterId, brokerId)
        .thenApply(broker -> checkEntityExists(broker, "Broker %d cannot be found.", brokerId))
        .thenCompose(
            broker -> {
              DescribeLogDirsResult result =
                  adminClient.describeLogDirs(
                      singletonList(brokerId), new DescribeLogDirsOptions());
              return KafkaFutures.toCompletableFuture(result.descriptions().get(brokerId));
            })
        .thenCompose(
            logDirs -> {
              log.debug("Describe log dirs {} ", logDirs);
              return CompletableFutures.allAsList(
                  logDirs.values().stream()
                      .flatMap(logDir -> logDir.replicaInfos().keySet().stream())
                      .map(
                          partition ->
                              getReplicaAllowMissing(
                                  clusterId, partition.topic(), partition.partition(), brokerId))
                      .collect(Collectors.toList()));
            })
        .thenApply(
            replicas ->
                replicas.stream()
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .collect(Collectors.toList()));
  }
}
