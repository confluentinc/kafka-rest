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
import static io.confluent.kafkarest.controllers.Entities.findEntityByKey;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static java.util.Objects.requireNonNull;

import io.confluent.kafkarest.common.KafkaFutures;
import io.confluent.kafkarest.entities.AbstractConfig;
import io.confluent.kafkarest.entities.AlterConfigCommand;
import io.confluent.kafkarest.entities.ConfigSource;
import io.confluent.kafkarest.entities.ConfigSynonym;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.ws.rs.NotFoundException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DescribeConfigsOptions;
import org.apache.kafka.common.config.ConfigResource;

/**
 * An abstract base class for managers of subtypes of {@link AbstractConfig}.
 */
abstract class AbstractConfigManager<
    T extends AbstractConfig, B extends AbstractConfig.Builder<T, B>> {

  private final Admin adminClient;
  private final ClusterManager clusterManager;

  AbstractConfigManager(Admin adminClient, ClusterManager clusterManager) {
    this.adminClient = requireNonNull(adminClient);
    this.clusterManager = requireNonNull(clusterManager);
  }

  final CompletableFuture<List<T>> listConfigs(
      String clusterId, ConfigResource resourceId, B prototype) {
    return clusterManager.getCluster(clusterId)
        .thenApply(cluster -> checkEntityExists(cluster, "Cluster %s cannot be found.", clusterId))
        .thenCompose(
            cluster ->
                KafkaFutures.toCompletableFuture(
                    adminClient.describeConfigs(
                        singletonList(resourceId),
                        new DescribeConfigsOptions().includeSynonyms(true))
                        .values()
                        .get(resourceId)))
        .thenApply(
            response ->
                response.entries().stream()
                    .map(
                        entry ->
                            prototype.setName(entry.name())
                                .setValue(entry.value())
                                .setDefault(entry.isDefault())
                                .setReadOnly(entry.isReadOnly())
                                .setSensitive(entry.isSensitive())
                                .setSource(ConfigSource.fromAdminConfigSource(entry.source()))
                                .setSynonyms(
                                    entry.synonyms().stream()
                                        .map(ConfigSynonym::fromAdminConfigSynonym)
                                        .collect(Collectors.toList()))
                                .build())
                    .collect(Collectors.toList()));
  }

  final CompletableFuture<Optional<T>> getConfig(
      String clusterId, ConfigResource resourceId, B prototype, String name) {
    return listConfigs(clusterId, resourceId, prototype)
        .thenApply(configs -> findEntityByKey(configs, AbstractConfig::getName, name));
  }

  /**
   * Updates the config {@code name} value to {@code newValue}, checking if the config exists first.
   */
  final CompletableFuture<Void> safeUpdateConfig(
      String clusterId, ConfigResource resourceId, B prototype, String name, String newValue) {
    return getConfig(clusterId, resourceId, prototype, name)
        .thenApply(
            config ->
                checkEntityExists(
                    config,
                    "Config %s cannot be found for %s %s in cluster %s.",
                    name,
                    resourceId.type(),
                    resourceId.name(),
                    clusterId))
        .thenCompose(
            config ->
                alterConfigs(resourceId, singletonList(AlterConfigCommand.set(name, newValue))));
  }

  /**
   * Updates the config {@code name} value to {@code newValue}, without checking if the config
   * exists first.
   */
  final CompletableFuture<Void> unsafeUpdateConfig(
      String clusterId, ConfigResource resourceId, String name, String newValue) {
    return clusterManager.getCluster(clusterId)
        .thenApply(cluster -> checkEntityExists(cluster, "Cluster %s cannot be found.", clusterId))
        .thenCompose(
            cluster ->
                alterConfigs(resourceId, singletonList(AlterConfigCommand.set(name, newValue))));
  }

  /**
   * Resets the config {@code name} to its default value, checking if the config exists first.
   */
  final CompletableFuture<Void> safeResetConfig(
      String clusterId, ConfigResource resourceId, B prototype, String name) {
    return getConfig(clusterId, resourceId, prototype, name)
        .thenApply(
            config ->
                checkEntityExists(
                    config,
                    "Config %s cannot be found for %s %s in cluster %s.",
                    name,
                    resourceId.type(),
                    resourceId.name(),
                    clusterId))
        .thenCompose(
            config -> alterConfigs(resourceId, singletonList(AlterConfigCommand.delete(name))));
  }

  /**
   * Resets the config {@code name} to its default value, without checking if the config exists
   * first.
   */
  final CompletableFuture<Void> unsafeResetConfig(
      String clusterId, ConfigResource resourceId, String name) {
    return clusterManager.getCluster(clusterId)
        .thenApply(cluster -> checkEntityExists(cluster, "Cluster %s cannot be found.", clusterId))
        .thenCompose(
            cluster -> alterConfigs(resourceId, singletonList(AlterConfigCommand.delete(name))));
  }

  /**
   * Atomically alter configs according to {@code commands}, checking if the configs exist first.
   */
  final CompletableFuture<Void> safeAlterConfigs(
      String clusterId, ConfigResource resourceId, B prototype, List<AlterConfigCommand> commands) {
    return listConfigs(clusterId, resourceId, prototype)
        .thenApply(
            configs -> {
              Set<String> configNames =
                  configs.stream().map(AbstractConfig::getName).collect(Collectors.toSet());
              for (AlterConfigCommand command : commands) {
                if (!configNames.contains(command.getName())) {
                  throw new NotFoundException(
                      String.format(
                          "Config %s cannot be found for %s %s in cluster %s.",
                          command.getName(),
                          resourceId.type(),
                          resourceId.name(),
                          clusterId));
                }
              }
              return configs;
            })
        .thenCompose(config -> alterConfigs(resourceId, commands));
  }

  /**
   * Atomically alter configs according to {@code commands}, without checking if the config exist
   * first.
   */
  final CompletableFuture<Void> unsafeAlterConfigs(
      String clusterId, ConfigResource resourceId, List<AlterConfigCommand> commands) {
    return clusterManager.getCluster(clusterId)
        .thenApply(cluster -> checkEntityExists(cluster, "Cluster %s cannot be found.", clusterId))
        .thenCompose(cluster -> alterConfigs(resourceId, commands));
  }

  private CompletableFuture<Void> alterConfigs(
      ConfigResource resourceId, List<AlterConfigCommand> commands) {
    return KafkaFutures.toCompletableFuture(
        adminClient.incrementalAlterConfigs(
            singletonMap(
                resourceId,
                commands.stream()
                    .map(AlterConfigCommand::toAlterConfigOp)
                    .collect(Collectors.toList())))
            .values()
            .get(resourceId));
  }
}
