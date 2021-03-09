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
import io.confluent.kafkarest.entities.BrokerConfig;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import javax.inject.Inject;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.config.ConfigResource;

final class BrokerConfigManagerImpl
    extends AbstractConfigManager<BrokerConfig, BrokerConfig.Builder>
    implements BrokerConfigManager {

  @Inject
  BrokerConfigManagerImpl(Admin adminClient, ClusterManager clusterManager) {
    super(adminClient, clusterManager);
  }

  @Override
  public CompletableFuture<List<BrokerConfig>> listBrokerConfigs(
      String clusterId, int brokerId) {
    return listConfigs(
        clusterId,
        new ConfigResource(ConfigResource.Type.BROKER, String.valueOf(brokerId)),
        BrokerConfig.builder().setClusterId(clusterId).setBrokerId(brokerId));
  }

  @Override
  public CompletableFuture<Optional<BrokerConfig>> getBrokerConfig(
      String clusterId, int brokerId, String name) {
    return getConfig(
        clusterId,
        new ConfigResource(ConfigResource.Type.BROKER, String.valueOf(brokerId)),
        BrokerConfig.builder().setClusterId(clusterId).setBrokerId(brokerId),
        name);
  }

  @Override
  public CompletableFuture<Void> updateBrokerConfig(
      String clusterId, int brokerId, String name, String newValue) {
    return safeUpdateConfig(
        clusterId,
        new ConfigResource(ConfigResource.Type.BROKER, String.valueOf(brokerId)),
        BrokerConfig.builder().setClusterId(clusterId).setBrokerId(brokerId),
        name,
        newValue);
  }

  @Override
  public CompletableFuture<Void> resetBrokerConfig(
      String clusterId, int brokerId, String name) {
    return safeResetConfig(
        clusterId,
        new ConfigResource(ConfigResource.Type.BROKER, String.valueOf(brokerId)),
        BrokerConfig.builder().setClusterId(clusterId).setBrokerId(brokerId),
        name);
  }

  @Override
  public CompletableFuture<Void> alterBrokerConfigs(
      String clusterId, int brokerId, List<AlterConfigCommand> commands) {
    return safeAlterConfigs(
        clusterId,
        new ConfigResource(ConfigResource.Type.BROKER, String.valueOf(brokerId)),
        BrokerConfig.builder().setClusterId(clusterId).setBrokerId(brokerId),
        commands);
  }
}
