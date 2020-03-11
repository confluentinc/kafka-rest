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

import io.confluent.kafkarest.entities.Partition;
import io.confluent.kafkarest.entities.PartitionReplica;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import javax.inject.Inject;
import javax.ws.rs.NotFoundException;

final class ReplicaManagerImpl implements ReplicaManager {

  private final PartitionManager partitionManager;

  @Inject
  ReplicaManagerImpl(PartitionManager partitionManager) {
    this.partitionManager = Objects.requireNonNull(partitionManager);
  }

  @Override
  public CompletableFuture<List<PartitionReplica>> listReplicas(
      String clusterId, String topicName, int partitionId) {
    return partitionManager.getPartition(clusterId, topicName, partitionId)
        .thenApply(
            partition ->
                partition.orElseThrow(
                    () -> new NotFoundException(
                        String.format(
                            "Partition %d of topic %s could not be found on cluster %s.",
                            partitionId,
                            topicName,
                            clusterId))))
        .thenApply(Partition::getReplicas);
  }

  @Override
  public CompletableFuture<Optional<PartitionReplica>> getReplica(
      String clusterId, String topicName, int partitionId, int brokerId) {
    return listReplicas(clusterId, topicName, partitionId)
        .thenApply(
            replicas ->
                replicas.stream()
                    .filter(replica -> replica.getBrokerId() == brokerId)
                    .findAny());
  }
}
