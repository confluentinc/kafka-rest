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

import io.confluent.kafkarest.entities.PartitionReplica;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * A service to manage Kafka {@link PartitionReplica Replicas}.
 */
public interface ReplicaManager {

  /**
   * Returns the list of Kafka {@link PartitionReplica Replicas} belonging to the {@link
   * io.confluent.kafkarest.entities.Partition} with the given {@code partitionId}.
   */
  CompletableFuture<List<PartitionReplica>> listReplicas(
      String clusterId, String topicName, int partitionId);

  /**
   * Returns the Kafka {@link PartitionReplica Replica} with the given {@code partitionId}.
   */
  CompletableFuture<Optional<PartitionReplica>> getReplica(
      String clusterId, String topicName, int partitionId, int brokerId);

  /**
   * Returns the list of Kafka {@link PartitionReplica Replicas} living on the {@link
   * io.confluent.kafkarest.entities.Broker} with the given {@code brokerId}.
   */
  CompletableFuture<List<PartitionReplica>> searchReplicasByBrokerId(
      String clusterId, int brokerId);
}
