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

import io.confluent.kafkarest.entities.BrokerConfig;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.inject.Inject;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.ConfigResource.Type;

public class BrokerConfigManagerImpl implements  BrokerConfigManager {

  private final Admin adminClient;
  private final ClusterManager clusterManager;

  @Inject
  BrokerConfigManagerImpl(Admin adminClient, ClusterManager clusterManager) {
    this.adminClient = adminClient;
    this.clusterManager = clusterManager;
  }


  @Override
  public CompletableFuture<List<BrokerConfig>> listBrokerConfigs(String clusterId,
      String brokerId) {
    ConfigResource resource = new ConfigResource(Type.BROKER, brokerId);
    return clusterManager.getCluster(clusterId)
    .thenApply(cluster -> checkEntityExists(cluster, "Cluster %s cannot be found.", clusterId))
    .thenCompose(
        broker ->
            KafkaFutures.toCompletableFuture(
                adminClient.describeConfigs(singletonList(resource)).values().get(resource)))
        .thenApply(
            config ->
                config.entries().stream()
                    .map(
                        entry ->
                            new BrokerConfig(
                                clusterId,
                                brokerId,
                                entry.name(),
                                entry.value(),
                                entry.isDefault(),
                                entry.isReadOnly(),
                                entry.isSensitive()))
                    .collect(Collectors.toList()));
  }

  @Override
  public CompletableFuture<Optional<BrokerConfig>> getBrokerConfig(String clusterId,
      String brokerId, String name) {
    return listBrokerConfigs(clusterId, brokerId)
        .thenApply(configs -> findEntityByKey(configs, BrokerConfig::getName, name));
  }
}
