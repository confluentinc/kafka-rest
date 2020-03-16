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

import io.confluent.kafkarest.entities.TopicConfiguration;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.inject.Inject;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.common.config.ConfigResource;

final class TopicConfigurationManagerImpl implements TopicConfigurationManager {

  private final Admin adminClient;
  private final TopicManager topicManager;

  @Inject
  TopicConfigurationManagerImpl(Admin adminClient, TopicManager topicManager) {
    this.adminClient = Objects.requireNonNull(adminClient);
    this.topicManager = Objects.requireNonNull(topicManager);
  }

  @Override
  public CompletableFuture<List<TopicConfiguration>> listTopicConfigurations(
      String clusterId, String topicName) {
    ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topicName);

    return topicManager.getTopic(clusterId, topicName)
        .thenApply(
            topic ->
                checkEntityExists(
                    topic, "Topic %s cannot be found in cluster %s.", topicName, clusterId))
        .thenCompose(
            topic ->
                KafkaFutures.toCompletableFuture(
                    adminClient.describeConfigs(singletonList(resource)).values().get(resource)))
        .thenApply(
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
                    .collect(Collectors.toList()));
  }

  @Override
  public CompletableFuture<Optional<TopicConfiguration>> getTopicConfiguration(
      String clusterId, String topicName, String name) {
    return listTopicConfigurations(clusterId, topicName)
        .thenApply(
            configurations -> findEntityByKey(configurations, TopicConfiguration::getName, name));
  }

  @Override
  public CompletableFuture<Void> updateTopicConfiguration(
      String clusterId, String topicName, String name, String newValue) {
    ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topicName);

    return getTopicConfiguration(clusterId, topicName, name)
        .thenApply(
            configuration ->
                checkEntityExists(
                    configuration,
                    "Configuration %s cannot be found for topic %s in cluster %s.",
                    name,
                    topicName,
                    clusterId))
        .thenCompose(
            topic ->
                KafkaFutures.toCompletableFuture(
                    adminClient.incrementalAlterConfigs(
                        singletonMap(
                            resource,
                            singletonList(
                                new AlterConfigOp(
                                    new ConfigEntry(name, newValue), AlterConfigOp.OpType.SET))))
                        .values()
                        .get(resource)));
  }

  @Override
  public CompletableFuture<Void> resetTopicConfiguration(
      String clusterId, String topicName, String name) {
    ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topicName);

    return getTopicConfiguration(clusterId, topicName, name)
        .thenApply(
            configuration ->
                checkEntityExists(
                    configuration,
                    "Configuration %s cannot be found for topic %s in cluster %s.",
                    name,
                    topicName,
                    clusterId))
        .thenCompose(
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
                        .get(resource)));
  }
}
