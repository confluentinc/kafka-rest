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
import javax.inject.Inject;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.config.ConfigResource;

// What is referred to as "cluster config" here is really the dynamic cluster-wide broker config
// as introduced in KIP-226. In terms of the Admin API, this is accessed using the BROKER resource
// type with the default entity name of "".
final class ClusterConfigManagerImpl
    extends AbstractConfigManager<ClusterConfig, ClusterConfig.Builder>
    implements ClusterConfigManager {

  private static final String BROKER_DEFAULT_ENTITY_NAME = "";

  @Inject
  ClusterConfigManagerImpl(Admin adminClient, ClusterManager clusterManager) {
    super(adminClient, clusterManager);
  }

  @Override
  public CompletableFuture<List<ClusterConfig>> listClusterConfigs(
      String clusterId, ClusterConfig.Type type) {
    return listConfigs(
        clusterId,
        new ConfigResource(type.getAdminType(), BROKER_DEFAULT_ENTITY_NAME),
        ClusterConfig.builder().setClusterId(clusterId).setType(type));
  }

  @Override
  public CompletableFuture<Optional<ClusterConfig>> getClusterConfig(
      String clusterId, ClusterConfig.Type type, String name) {
    return getConfig(
        clusterId,
        new ConfigResource(type.getAdminType(), BROKER_DEFAULT_ENTITY_NAME),
        ClusterConfig.builder().setClusterId(clusterId).setType(type),
        name);
  }

  @Override
  public CompletableFuture<Void> upsertClusterConfig(
      String clusterId, ClusterConfig.Type type, String name, String newValue) {
    // Since listing cluster configs will only return the ones dynamically created, there's no way
    // currently of knowing which config names are valid to create/update. So we skip the existence
    // check. If the config is not valid, it will fail silently.
    return unsafeUpdateConfig(
        clusterId,
        new ConfigResource(type.getAdminType(), BROKER_DEFAULT_ENTITY_NAME),
        name,
        newValue);
  }

  @Override
  public CompletableFuture<Void> deleteClusterConfig(
      String clusterId, ClusterConfig.Type type, String name) {
    return safeResetConfig(
        clusterId,
        new ConfigResource(type.getAdminType(), BROKER_DEFAULT_ENTITY_NAME),
        ClusterConfig.builder().setClusterId(clusterId).setType(type),
        name);
  }

  @Override
  public CompletableFuture<Void> alterClusterConfigs(
      String clusterId, ClusterConfig.Type type, List<AlterConfigCommand> commands) {
    return unsafeAlterConfigs(
        clusterId, new ConfigResource(type.getAdminType(), BROKER_DEFAULT_ENTITY_NAME), commands);
  }
}
