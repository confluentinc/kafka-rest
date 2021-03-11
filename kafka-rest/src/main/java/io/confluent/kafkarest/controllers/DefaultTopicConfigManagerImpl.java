/*
 * Copyright 2021 Confluent Inc.
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

import io.confluent.kafkarest.common.KafkaFutures;
import io.confluent.kafkarest.entities.AbstractConfig;
import io.confluent.kafkarest.entities.BrokerConfig;
import io.confluent.kafkarest.entities.ConfigSource;
import io.confluent.kafkarest.entities.ConfigSynonym;
import io.confluent.kafkarest.entities.TopicConfig;
import kafka.server.KafkaConfig;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.CreateTopicsOptions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.errors.UnsupportedVersionException;

import javax.inject.Inject;
import javax.ws.rs.NotFoundException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.stream.Collectors;

import static io.confluent.kafkarest.controllers.Entities.checkEntityExists;
import static io.confluent.kafkarest.controllers.Entities.findEntityByKey;
import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;

final class DefaultTopicConfigManagerImpl implements DefaultTopicConfigManager {

  private final Admin adminClient;
  private final ClusterManager clusterManager;
  private final BrokerConfigManager brokerConfigManager;
  private final BrokerManager brokerManager;

  @Inject
  DefaultTopicConfigManagerImpl(Admin adminClient,
                                ClusterManager clusterManager,
                                BrokerConfigManager brokerConfigManager,
                                BrokerManager brokerManager) {
    this.adminClient = requireNonNull(adminClient);
    this.clusterManager = requireNonNull(clusterManager);
    this.brokerConfigManager = requireNonNull(brokerConfigManager);
    this.brokerManager = requireNonNull(brokerManager);
  }

  @Override
  public CompletableFuture<List<TopicConfig>> listDefaultTopicConfigs(
      String clusterId, String topicName) {
    return clusterManager.getCluster(clusterId)
        .thenApply(cluster -> checkEntityExists(cluster, "Cluster %s cannot be found.", clusterId))
        .thenCompose(
            cluster ->
                KafkaFutures.toCompletableFuture(
                    adminClient.createTopics(
                        singletonList(new NewTopic(topicName, Optional.empty(), Optional.empty())),
                        new CreateTopicsOptions().validateOnly(true)
                    ).config(topicName)))
        .handle((result, ex) -> {
              // InvalidRequestException can happen in the case of multi-tenancy, where passing
              // Optional.empty() is not supported and will results in exception (i.e. 'Invalid
              // partition count -1').
              // UnsupportedVersionException can happen in the case of older brokers
              // For such cases we explicitly pass partition and replicationFactor
              if (ex != null) {
                if (ex.getCause() instanceof InvalidRequestException
                    || ex.getCause() instanceof UnsupportedVersionException) {
                  CompletableFuture<Config> future = brokerManager.listBrokers(clusterId)
                      .thenCompose(
                          brokers ->
                              brokerConfigManager.listBrokerConfigs(
                                  clusterId,
                                  brokers.stream().findFirst().get().getBrokerId())
                                  .thenCompose(
                                      brokerConfig -> {
                                        int partitions = Integer.parseInt(getConfigValue(
                                            brokerConfig,
                                            KafkaConfig.NumPartitionsProp()));
                                        short replicationFactor = Short.parseShort(getConfigValue(
                                            brokerConfig,
                                            KafkaConfig.DefaultReplicationFactorProp()));
                                        return KafkaFutures.toCompletableFuture(
                                            adminClient.createTopics(
                                                singletonList(new NewTopic(
                                                    topicName,
                                                    partitions,
                                                    replicationFactor)),
                                                new CreateTopicsOptions()
                                                    .validateOnly(true))
                                                .config(topicName));
                                      }
                                  )
                      );
                  return future;
                }
                if (ex instanceof RuntimeException) {
                  throw (RuntimeException) ex;
                }
                throw new CompletionException(ex);
              }
              return CompletableFuture.completedFuture(result);
        }
        )
        .thenCompose(
            handled -> handled.thenApply(
                response ->
                    response.entries().stream()
                        .map(
                            entry ->
                                TopicConfig.builder().setClusterId(clusterId)
                                    .setTopicName(topicName)
                                    .setName(entry.name())
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
                        .collect(Collectors.toList())
            ));
  }

  private String getConfigValue(List<BrokerConfig> brokerConfigs, String configName) {
    return findEntityByKey(
        brokerConfigs,
        AbstractConfig::getName,
        configName)
        .orElseThrow(() -> new NotFoundException(
            String.format("Could not determine required broker configuration: %s",
                configName)))
        .getValue();
  }
}
