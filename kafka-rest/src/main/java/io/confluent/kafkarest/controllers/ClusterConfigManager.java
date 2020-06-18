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

import io.confluent.kafkarest.entities.AlterConfigCommand;
import io.confluent.kafkarest.entities.ClusterConfig;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public interface ClusterConfigManager {

  /**
   * Returns the list of Kafka {@link ClusterConfig ClusterConfigs}.
   */
  CompletableFuture<List<ClusterConfig>> listClusterConfigs(
      String clusterId, ClusterConfig.Type type);

  /**
   * Returns the Kafka {@link ClusterConfig} with the given {@code name}.
   */
  CompletableFuture<Optional<ClusterConfig>> getClusterConfig(
      String clusterId, ClusterConfig.Type type, String name);

  /**
   * Creates or updates the Kafka {@link ClusterConfig} with the given {@code name} with the given
   * {@code newValue}.
   */
  CompletableFuture<Void> upsertClusterConfig(
      String clusterId, ClusterConfig.Type type, String name, String newValue);

  /**
   * Resets the Kafka {@link ClusterConfig} with the given {@code name} to its default value.
   */
  CompletableFuture<Void> deleteClusterConfig(
      String clusterId, ClusterConfig.Type type, String name);

  /**
   * Atomically alters the Kafka {@link ClusterConfig Cluster Configs} according to {@code
   * commands}.
   */
  CompletableFuture<Void> alterClusterConfigs(
      String clusterId, ClusterConfig.Type type, List<AlterConfigCommand> commands);
}
