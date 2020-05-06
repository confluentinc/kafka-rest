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
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * A service to manage Kafka {@link Partition Partitions}.
 */
public interface PartitionManager {

  /**
   * Returns the list of Kafka {@link Partition Partitions} belonging to the {@link
   * io.confluent.kafkarest.entities.Topic} with the given {@code topicName}.
   */
  CompletableFuture<List<Partition>> listPartitions(String clusterId, String topicName);

  /**
   * Returns the list of Kafka {@link Partition Partitions} belonging to the {@link
   * io.confluent.kafkarest.entities.Topic} with the given {@code topicName}, in the {@link
   * io.confluent.kafkarest.entities.Cluster} that this application is connected to.
   */
  CompletableFuture<List<Partition>> listLocalPartitions(String topicName);

  /**
   * Returns the Kafka {@link Partition} with the given {@code partitionId}.
   */
  CompletableFuture<Optional<Partition>> getPartition(
      String clusterId, String topicName, int partitionId);

  /**
   * Returns the Kafka {@link Partition} with the given {@code partitionId}, belonging to the {@link
   * io.confluent.kafkarest.entities.Cluster} that this application is connected to.
   */
  CompletableFuture<Optional<Partition>> getLocalPartition(String topicName, int partitionId);
}
