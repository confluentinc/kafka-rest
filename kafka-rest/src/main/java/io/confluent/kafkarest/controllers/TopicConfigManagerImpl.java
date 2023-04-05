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
import io.confluent.kafkarest.entities.TopicConfig;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import javax.inject.Inject;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.config.ConfigResource;

final class TopicConfigManagerImpl
    extends AbstractConfigManager<TopicConfig, TopicConfig.Builder> implements TopicConfigManager {

  @Inject
  TopicConfigManagerImpl(Admin adminClient, ClusterManager clusterManager) {
    super(adminClient, clusterManager);
  }

  @Override
  public CompletableFuture<List<TopicConfig>> listTopicConfigs(
      String clusterId, String topicName) {
    return listConfigs(
        clusterId,
        new ConfigResource(ConfigResource.Type.TOPIC, topicName),
        TopicConfig.builder().setClusterId(clusterId).setTopicName(topicName));
  }

  @Override
  public CompletableFuture<Optional<TopicConfig>> getTopicConfig(
      String clusterId, String topicName, String name) {
    return getConfig(
        clusterId,
        new ConfigResource(ConfigResource.Type.TOPIC, topicName),
        TopicConfig.builder().setClusterId(clusterId).setTopicName(topicName),
        name);
  }

  @Override
  public CompletableFuture<Void> updateTopicConfig(
      String clusterId, String topicName, String name, String newValue) {
    return safeUpdateConfig(
        clusterId,
        new ConfigResource(ConfigResource.Type.TOPIC, topicName),
        TopicConfig.builder().setClusterId(clusterId).setTopicName(topicName),
        name,
        newValue);
  }

  @Override
  public CompletableFuture<Void> resetTopicConfig(
      String clusterId, String topicName, String name) {
    return safeResetConfig(
        clusterId,
        new ConfigResource(ConfigResource.Type.TOPIC, topicName),
        TopicConfig.builder().setClusterId(clusterId).setTopicName(topicName),
        name);
  }

  @Override
  public CompletableFuture<Void> alterTopicConfigs(
      String clusterId, String topicName, List<AlterConfigCommand> commands) {
    return safeAlterConfigs(
        clusterId,
        new ConfigResource(ConfigResource.Type.TOPIC, topicName),
        TopicConfig.builder().setClusterId(clusterId).setTopicName(topicName),
        commands);
  }
}
