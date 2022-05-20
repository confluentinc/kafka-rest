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
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.inject.Inject;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.ConfigResource.Type;

final class BrokerConfigManagerImpl
    extends AbstractConfigManager<BrokerConfig, BrokerConfig.Builder>
    implements BrokerConfigManager {

  @Inject
  BrokerConfigManagerImpl(Admin adminClient, ClusterManager clusterManager) {
    super(adminClient, clusterManager);
  }

  @Override
  public CompletableFuture<List<BrokerConfig>> listBrokerConfigs(String clusterId, int brokerId) {
    return listConfigs(
        clusterId,
        new ConfigResource(ConfigResource.Type.BROKER, String.valueOf(brokerId)),
        BrokerConfig.builder().setClusterId(clusterId).setBrokerId(brokerId));
  }

  @Override
  public CompletableFuture<Map<Integer, List<BrokerConfig>>> listAllBrokerConfigs(
      String clusterId, List<Integer> brokerIds) {
    List<ConfigResource> brokerResources =
        brokerIds.stream()
            .map(brokerId -> new ConfigResource(Type.BROKER, String.valueOf(brokerId)))
            .collect(Collectors.toList());
    return listConfigs(
            clusterId,
            brokerResources,
            BrokerConfig.builder().setClusterId(clusterId).setBrokerId(-1))
        .thenApply(
            configs ->
                configs.entrySet().stream()
                    .collect(
                        Collectors.toMap(
                            e -> Integer.parseInt(e.getKey().name()),
                            e ->
                                e.getValue().stream()
                                    .map(
                                        config ->
                                            BrokerConfig.create(
                                                config.getClusterId(),
                                                Integer.parseInt(e.getKey().name()),
                                                config.getName(),
                                                config.getValue(),
                                                config.isDefault(),
                                                config.isReadOnly(),
                                                config.isSensitive(),
                                                config.getSource(),
                                                config.getSynonyms()))
                                    .collect(Collectors.toList()))));
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
  public CompletableFuture<Void> resetBrokerConfig(String clusterId, int brokerId, String name) {
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
