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
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

import io.confluent.kafkarest.concurrent.NonBlockingExecutor;
import io.confluent.kafkarest.entities.Partition;
import io.confluent.kafkarest.entities.PartitionReplica;
import io.confluent.kafkarest.entities.Topic;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;
import javax.inject.Inject;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;

final class TopicManagerImpl implements TopicManager {

  private final Admin adminClient;
  private final ClusterManager clusterManager;
  private final ExecutorService executor;

  @Inject
  TopicManagerImpl(
      Admin adminClient,
      ClusterManager clusterManager,
      @NonBlockingExecutor ExecutorService executor) {
    this.adminClient = Objects.requireNonNull(adminClient);
    this.clusterManager = Objects.requireNonNull(clusterManager);
    this.executor = Objects.requireNonNull(executor);
  }

  @Override
  public CompletableFuture<List<Topic>> listTopics(String clusterId) {
    return clusterManager.getCluster(clusterId)
        .thenApplyAsync(
            cluster -> checkEntityExists(cluster, "Cluster %s cannot be found.", clusterId),
            executor)
        .thenComposeAsync(
            cluster -> KafkaFutures.toCompletableFuture(adminClient.listTopics().listings()),
            executor)
        .thenComposeAsync(
            topicListings -> {
              if (topicListings == null) {
                return CompletableFuture.completedFuture(emptyList());
              }
              return describeTopics(
                  clusterId,
                  topicListings.stream().map(TopicListing::name).collect(Collectors.toList()));
            },
            executor);
  }

  @Override
  public CompletableFuture<Optional<Topic>> getTopic(String clusterId, String topicName) {
    Objects.requireNonNull(topicName);

    return clusterManager.getCluster(clusterId)
        .thenApplyAsync(
            cluster -> checkEntityExists(cluster, "Cluster %s cannot be found.", clusterId),
            executor)
        .thenComposeAsync(cluster -> describeTopics(clusterId, singletonList(topicName)), executor)
        .thenApplyAsync(
            topics -> {
              if (topics == null || topics.isEmpty()) {
                return Optional.empty();
              }
              if (topics.size() > 1) {
                throw new IllegalStateException(
                    String.format(
                        "More than one topic exists with name %s in cluster %s.",
                        topicName,
                        clusterId));
              }
              return Optional.of(topics.get(0));
            },
            executor);
  }

  private CompletableFuture<List<Topic>> describeTopics(String clusterId, List<String> topicNames) {
    return KafkaFutures.toCompletableFuture(adminClient.describeTopics(topicNames).all())
        .thenApplyAsync(
            topics ->
                topics.values().stream()
                    .map(topicDescription -> toTopic(clusterId, topicDescription))
                    .collect(Collectors.toList()),
            executor);
  }

  private static Topic toTopic(String clusterId, TopicDescription topicDescription) {
    return new Topic(
        clusterId,
        topicDescription.name(),
        topicDescription.partitions().stream()
            .map(partition -> toPartition(clusterId, topicDescription.name(), partition))
            .collect(Collectors.toList()),
        (short) topicDescription.partitions().get(0).replicas().size(),
        topicDescription.isInternal());
  }

  private static Partition toPartition(
      String clusterId, String topicName, TopicPartitionInfo partitionInfo) {
    Set<Node> inSyncReplicas = new HashSet<>(partitionInfo.isr());
    List<PartitionReplica> replicas = new ArrayList<>();
    for (Node replica : partitionInfo.replicas()) {
      replicas.add(
          new PartitionReplica(
              clusterId,
              topicName,
              partitionInfo.partition(),
              replica.id(),
              partitionInfo.leader().equals(replica),
              inSyncReplicas.contains(replica)));
    }
    return new Partition(clusterId, topicName, partitionInfo.partition(), replicas);
  }

  @Override
  public CompletableFuture<Void> createTopic(
      String clusterId,
      String topicName,
      int partitionsCount,
      short replicationFactor,
      Map<String, String> configurations) {
    Objects.requireNonNull(topicName);

    NewTopic createTopicRequest =
        new NewTopic(topicName, partitionsCount, replicationFactor).configs(configurations);

    return clusterManager.getCluster(clusterId)
        .thenApplyAsync(
            cluster -> checkEntityExists(cluster, "Cluster %s cannot be found.", clusterId),
            executor)
        .thenComposeAsync(
            cluster ->
                KafkaFutures.toCompletableFuture(
                    adminClient.createTopics(singletonList(createTopicRequest)).all()),
            executor);
  }

  @Override
  public CompletableFuture<Void> deleteTopic(String clusterId, String topicName) {
    Objects.requireNonNull(topicName);

    return clusterManager.getCluster(clusterId)
        .thenApplyAsync(
            cluster -> checkEntityExists(cluster, "Cluster %s cannot be found.", clusterId),
            executor)
        .thenComposeAsync(
            cluster ->
                KafkaFutures.toCompletableFuture(
                    adminClient.deleteTopics(singletonList(topicName)).all()),
            executor);
  }
}
