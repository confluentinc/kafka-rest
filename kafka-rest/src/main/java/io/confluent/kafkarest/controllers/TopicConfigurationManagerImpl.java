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

import io.confluent.kafkarest.concurrent.NonBlockingExecutor;
import io.confluent.kafkarest.entities.TopicConfiguration;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;
import javax.inject.Inject;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.common.config.ConfigResource;

final class TopicConfigurationManagerImpl implements TopicConfigurationManager {

  private final Admin adminClient;
  private final ClusterManager clusterManager;
  private final ExecutorService executor;

  @Inject
  TopicConfigurationManagerImpl(
      Admin adminClient,
      ClusterManager clusterManager,
      @NonBlockingExecutor ExecutorService executor) {
    this.adminClient = Objects.requireNonNull(adminClient);
    this.clusterManager = Objects.requireNonNull(clusterManager);
    this.executor = Objects.requireNonNull(executor);
  }

  @Override
  public CompletableFuture<List<TopicConfiguration>> listTopicConfigurations(
      String clusterId, String topicName) {
    ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topicName);

    return clusterManager.getCluster(clusterId)
        .thenApplyAsync(
            cluster -> checkEntityExists(cluster, "Cluster %s cannot be found.", clusterId),
            executor)
        .thenComposeAsync(
            topic ->
                KafkaFutures.toCompletableFuture(
                    adminClient.describeConfigs(singletonList(resource)).values().get(resource)),
            executor)
        .thenApplyAsync(
            config ->
                config.entries().stream()
                    .map(
                        entry ->
                            new TopicConfiguration(
                                clusterId,
                                topicName,
                                entry.name(),
                                entry.value(),
                                entry.isDefault(),
                                entry.isReadOnly(),
                                entry.isSensitive()))
                    .collect(Collectors.toList()),
            executor);
  }

  @Override
  public CompletableFuture<Optional<TopicConfiguration>> getTopicConfiguration(
      String clusterId, String topicName, String name) {
    return listTopicConfigurations(clusterId, topicName)
        .thenApplyAsync(
            configurations -> findEntityByKey(configurations, TopicConfiguration::getName, name),
            executor);
  }

  @Override
  public CompletableFuture<Void> updateTopicConfiguration(
      String clusterId, String topicName, String name, String newValue) {
    ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topicName);

    return getTopicConfiguration(clusterId, topicName, name)
        .thenApplyAsync(
            configuration ->
                checkEntityExists(
                    configuration,
                    "Configuration %s cannot be found for topic %s in cluster %s.",
                    name,
                    topicName,
                    clusterId),
            executor)
        .thenComposeAsync(
            topic ->
                KafkaFutures.toCompletableFuture(
                    adminClient.incrementalAlterConfigs(
                        singletonMap(
                            resource,
                            singletonList(
                                new AlterConfigOp(
                                    new ConfigEntry(name, newValue), AlterConfigOp.OpType.SET))))
                        .values()
                        .get(resource)),
            executor);
  }

  @Override
  public CompletableFuture<Void> resetTopicConfiguration(
      String clusterId, String topicName, String name) {
    ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topicName);

    return getTopicConfiguration(clusterId, topicName, name)
        .thenApplyAsync(
            configuration ->
                checkEntityExists(
                    configuration,
                    "Configuration %s cannot be found for topic %s in cluster %s.",
                    name,
                    topicName,
                    clusterId),
            executor)
        .thenComposeAsync(
            topic ->
                KafkaFutures.toCompletableFuture(
                    adminClient.incrementalAlterConfigs(
                        singletonMap(
                            resource,
                            singletonList(
                                new AlterConfigOp(
                                    new ConfigEntry(name, /* value= */ null),
                                    AlterConfigOp.OpType.DELETE))))
                        .values()
                        .get(resource)),
            executor);
  }
}
