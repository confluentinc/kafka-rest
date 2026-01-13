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
import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;

import io.confluent.kafkarest.common.KafkaFutures;
import io.confluent.kafkarest.entities.ConsumerGroup;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.common.GroupState;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.errors.GroupAuthorizationException;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class ConsumerGroupManagerImpl implements ConsumerGroupManager {

  private static final Logger log = LoggerFactory.getLogger(ConsumerGroupManagerImpl.class);

  private final Admin adminClient;
  private final ClusterManager clusterManager;

  @Inject
  ConsumerGroupManagerImpl(Admin adminClient, ClusterManager clusterManager) {
    this.adminClient = requireNonNull(adminClient);
    this.clusterManager = requireNonNull(clusterManager);
  }

  @Override
  public CompletableFuture<List<ConsumerGroup>> listConsumerGroups(String clusterId) {
    return clusterManager
        .getCluster(clusterId)
        .thenApply(
            cluster -> checkEntityExists(cluster, "Cluster %s could not be found.", clusterId))
        .thenCompose(
            cluster -> KafkaFutures.toCompletableFuture(adminClient.listConsumerGroups().all()))
        .thenCompose(
            listings ->
                getConsumerGroups(
                    clusterId,
                    listings.stream()
                        .map(ConsumerGroupListing::groupId)
                        .collect(Collectors.toList())));
  }

  @Override
  public CompletableFuture<Optional<ConsumerGroup>> getConsumerGroup(
      String clusterId, String consumerGroupId) {
    return clusterManager
        .getCluster(clusterId)
        .thenApply(
            cluster -> checkEntityExists(cluster, "Cluster %s could not be found.", clusterId))
        .thenCompose(cluster -> getConsumerGroups(clusterId, singletonList(consumerGroupId)))
        .thenApply(consumerGroups -> consumerGroups.stream().findAny());
  }

  private CompletableFuture<List<ConsumerGroup>> getConsumerGroups(
      String clusterId, List<String> consumerGroupIds) {

    Map<String, KafkaFuture<ConsumerGroupDescription>> groupFutures =
        adminClient.describeConsumerGroups(consumerGroupIds).describedGroups();

    List<CompletableFuture<Optional<ConsumerGroup>>> futures =
        groupFutures.entrySet().stream()
            .map(entry -> describeGroupWithFallback(clusterId, entry.getKey(), entry.getValue()))
            .collect(Collectors.toList());

    return collectSuccessfulResults(futures);
  }

  private CompletableFuture<Optional<ConsumerGroup>> describeGroupWithFallback(
      String clusterId, String groupId, KafkaFuture<ConsumerGroupDescription> future) {

    return KafkaFutures.toCompletableFuture(future)
        .thenApply(description -> toConsumerGroup(clusterId, description))
        .exceptionally(error -> handleDescribeError(groupId, error));
  }

  private Optional<ConsumerGroup> toConsumerGroup(
      String clusterId, ConsumerGroupDescription description) {

    // Kafka returns a dummy description for non-existent groups (simple=true, state=DEAD)
    boolean isNonExistentGroup =
        description.isSimpleConsumerGroup() && description.groupState() == GroupState.DEAD;

    if (isNonExistentGroup) {
      return Optional.empty();
    }

    return Optional.of(ConsumerGroup.fromConsumerGroupDescription(clusterId, description));
  }


  private Optional<ConsumerGroup> handleDescribeError(String groupId, Throwable error) {
    if (hasCause(error, GroupAuthorizationException.class)) {
      log.debug("Skipping consumer group '{}' due to authorization failure", groupId);
      return Optional.empty();
    }

    if (hasCause(error, GroupIdNotFoundException.class)) {
      return Optional.empty();
    }

    throw new CompletionException(error);
  }

  private CompletableFuture<List<ConsumerGroup>> collectSuccessfulResults(
      List<CompletableFuture<Optional<ConsumerGroup>>> futures) {

    return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
        .thenApply(
            ignored ->
                futures.stream()
                    .map(CompletableFuture::join)
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .collect(Collectors.toList()));
  }

  private static boolean hasCause(Throwable ex, Class<? extends Throwable> causeType) {
    for (Throwable cause = ex; cause != null; cause = cause.getCause()) {
      if (causeType.isInstance(cause)) {
        return true;
      }
    }
    return false;
  }
}
