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
import static java.util.Collections.emptySet;
import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;

import io.confluent.kafkarest.common.KafkaFutures;
import io.confluent.kafkarest.entities.Acl;
import io.confluent.kafkarest.entities.Partition;
import io.confluent.kafkarest.entities.PartitionReplica;
import io.confluent.kafkarest.entities.Topic;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.inject.Inject;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.CreateTopicsOptions;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DescribeTopicsOptions;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;

final class TopicManagerImpl implements TopicManager {

  private final Admin adminClient;
  private final ClusterManager clusterManager;

  @Inject
  TopicManagerImpl(Admin adminClient, ClusterManager clusterManager) {
    this.adminClient = requireNonNull(adminClient);
    this.clusterManager = requireNonNull(clusterManager);
  }

  @Override
  public CompletableFuture<List<Topic>> listTopics(
      String clusterId, boolean includeAuthorizedOperations) {
    return clusterManager
        .getCluster(clusterId)
        .thenApply(cluster -> checkEntityExists(cluster, "Cluster %s cannot be found.", clusterId))
        .thenCompose(
            cluster -> KafkaFutures.toCompletableFuture(adminClient.listTopics().listings()))
        .thenCompose(
            topicListings -> {
              if (topicListings == null) {
                return CompletableFuture.completedFuture(emptyList());
              }
              return describeTopics(
                  clusterId,
                  topicListings.stream().map(TopicListing::name).collect(Collectors.toList()),
                  includeAuthorizedOperations);
            });
  }

  @Override
  public CompletableFuture<List<Topic>> listLocalTopics() {
    return clusterManager
        .getLocalCluster()
        .thenCompose(
            cluster ->
                KafkaFutures.toCompletableFuture(adminClient.listTopics().listings())
                    .thenCompose(
                        topicListings -> {
                          if (topicListings == null) {
                            return CompletableFuture.completedFuture(emptyList());
                          }
                          return describeTopics(
                              cluster.getClusterId(),
                              topicListings.stream()
                                  .map(TopicListing::name)
                                  .collect(Collectors.toList()),
                              false);
                        }));
  }

  @Override
  public CompletableFuture<Optional<Topic>> getTopic(
      String clusterId, String topicName, boolean includeAuthorizedOperations) {
    requireNonNull(topicName);

    return clusterManager
        .getCluster(clusterId)
        .thenApply(cluster -> checkEntityExists(cluster, "Cluster %s cannot be found.", clusterId))
        .thenCompose(
            cluster ->
                describeTopics(clusterId, singletonList(topicName), includeAuthorizedOperations))
        .thenApply(
            topics -> {
              if (topics == null || topics.isEmpty()) {
                return Optional.empty();
              }
              if (topics.size() > 1) {
                throw new IllegalStateException(
                    String.format(
                        "More than one topic exists with name %s in cluster %s.",
                        topicName, clusterId));
              }
              return Optional.of(topics.get(0));
            });
  }

  @Override
  public CompletableFuture<Optional<Topic>> getLocalTopic(String topicName) {
    requireNonNull(topicName);

    return clusterManager
        .getLocalCluster()
        .thenCompose(
            cluster -> describeTopics(cluster.getClusterId(), singletonList(topicName), false))
        .thenApply(
            topics -> {
              if (topics == null || topics.isEmpty()) {
                return Optional.empty();
              }
              if (topics.size() > 1) {
                throw new IllegalStateException(
                    String.format("More than one topic exists with name %s.", topicName));
              }
              return Optional.of(topics.get(0));
            });
  }

  private CompletableFuture<List<Topic>> describeTopics(
      String clusterId, List<String> topicNames, boolean includeAuthorizedOperations) {
    return KafkaFutures.toCompletableFuture(
            adminClient
                .describeTopics(
                    topicNames,
                    new DescribeTopicsOptions()
                        .includeAuthorizedOperations(includeAuthorizedOperations))
                .allTopicNames())
        .thenApply(
            topics ->
                topics.values().stream()
                    .map(topicDescription -> toTopic(clusterId, topicDescription))
                    .collect(Collectors.toList()));
  }

  private static Topic toTopic(String clusterId, TopicDescription topicDescription) {
    return Topic.create(
        clusterId,
        topicDescription.name(),
        topicDescription.partitions().stream()
            .map(partition -> toPartition(clusterId, topicDescription.name(), partition))
            .collect(Collectors.toList()),
        (short) topicDescription.partitions().get(0).replicas().size(),
        topicDescription.isInternal(),
        topicDescription.authorizedOperations() == null
            ? emptySet()
            : topicDescription.authorizedOperations().stream()
                .map(Acl.Operation::fromAclOperation)
                .collect(Collectors.toSet()));
  }

  private static Partition toPartition(
      String clusterId, String topicName, TopicPartitionInfo partitionInfo) {
    Set<Node> inSyncReplicas = new HashSet<>(partitionInfo.isr());
    List<PartitionReplica> replicas = new ArrayList<>();
    for (Node replica : partitionInfo.replicas()) {
      replicas.add(
          PartitionReplica.create(
              clusterId,
              topicName,
              partitionInfo.partition(),
              replica.id(),
              replica.equals(partitionInfo.leader()),
              inSyncReplicas.contains(replica)));
    }
    return Partition.create(clusterId, topicName, partitionInfo.partition(), replicas);
  }

  @Override
  public CompletableFuture<Void> createTopic(
      String clusterId,
      String topicName,
      Optional<Integer> partitionsCount,
      Optional<Short> replicationFactor,
      Map<Integer, List<Integer>> replicasAssignments,
      Map<String, Optional<String>> configs) {
    return createTopic2(
            clusterId,
            topicName,
            partitionsCount,
            replicationFactor,
            replicasAssignments,
            configs,
            false)
        .thenAccept(
            unused -> {
              return;
            });
  }

  @Override
  public CompletableFuture<Topic> createTopic2(
      String clusterId,
      String topicName,
      Optional<Integer> partitionsCount,
      Optional<Short> replicationFactor,
      Map<Integer, List<Integer>> replicasAssignments,
      Map<String, Optional<String>> configs,
      boolean validateOnly) {
    requireNonNull(topicName);

    Map<String, String> nullableConfigs = new HashMap<>();
    configs.forEach((key, value) -> nullableConfigs.put(key, value.orElse(null)));

    // A new topic can be created with either uniform replication according to the given partitions
    // count and replication factor, or explicitly specified partition-to-replicas assignments.
    NewTopic createTopicRequest =
        replicasAssignments.isEmpty()
            ? new NewTopic(topicName, partitionsCount, replicationFactor).configs(nullableConfigs)
            : new NewTopic(topicName, replicasAssignments).configs(nullableConfigs);

    return clusterManager
        .getCluster(clusterId)
        .thenApply(cluster -> checkEntityExists(cluster, "Cluster %s cannot be found.", clusterId))
        .thenCompose(
            cluster ->
                createTopicInternal(
                    clusterId,
                    topicName,
                    createTopicRequest,
                    partitionsCount,
                    replicationFactor,
                    validateOnly));
  }

  private CompletableFuture<Topic> createTopicInternal(
      String clusterId,
      String topicName,
      NewTopic createTopicRequest,
      Optional<Integer> requestPartitionsCount,
      Optional<Short> requestReplicationFactor,
      boolean validateOnly) {

    CreateTopicsResult createTopicsResult =
        adminClient.createTopics(
            singletonList(createTopicRequest),
            new CreateTopicsOptions().validateOnly(validateOnly));

    return CompletableFuture.completedFuture(Topic.builder())
        .thenCombine(
            KafkaFutures.toCompletableFuture(createTopicsResult.all()),
            (topicBuilder, unused) -> {
              return topicBuilder
                  .setClusterId(clusterId)
                  .setName(topicName)
                  .setInternal(false)
                  .setAuthorizedOperations(emptySet());
            })
        .thenCombine(
            extractConfigFromResultUnlessNotAuthorized(
                createTopicsResult.numPartitions(topicName), requestPartitionsCount),
            (topicBuilder, responseNumPartitions) -> {
              ArrayList<Partition> dummyPartitions = new ArrayList<>(responseNumPartitions);
              for (int i = 0; i < responseNumPartitions; i++) {
                dummyPartitions.add(Partition.create(clusterId, topicName, i, emptyList()));
              }
              return topicBuilder.addAllPartitions(dummyPartitions);
            })
        .thenCombine(
            extractConfigFromResultUnlessNotAuthorized(
                createTopicsResult.replicationFactor(topicName), requestReplicationFactor),
            (topicBuilder, responseReplicationFactor) -> {
              return topicBuilder.setReplicationFactor(responseReplicationFactor.shortValue());
            })
        .thenApply(Topic.Builder::build);
  }

  // In KIP-525, the behavior of the CreateTopicsResult is that the config, including the
  // replication factor and number of partitions, is only returned if the creating principal had
  // DESCRIBE_CONFIGS authority. If it does not, the methods throw TopicAuthorizationException, even
  // when the request to create the topic actually succeeded. So, we need to make sure that these
  // exceptions do not fail the entire request, because the creation has actually worked. If the
  // value cannot be extracted from the actual result, we take the value from the original request,
  // if supplied, or else zero. The schema for the response says that the values are required, so
  // just omitting them if not present is not allowed.
  private CompletableFuture<Integer> extractConfigFromResultUnlessNotAuthorized(
      KafkaFuture<Integer> configFuture, Optional<? extends Number> defaultValue) {
    CompletableFuture<Integer> completableFuture = new CompletableFuture<>();
    configFuture.whenComplete(
        (value, exception) -> {
          if (exception == null) {
            completableFuture.complete(value);
          } else if (defaultValue.isPresent()) {
            completableFuture.complete(defaultValue.get().intValue());
          } else {
            completableFuture.complete(0);
          }
        });
    return completableFuture;
  }

  @Override
  public CompletableFuture<Void> deleteTopic(String clusterId, String topicName) {
    requireNonNull(topicName);

    return clusterManager
        .getCluster(clusterId)
        .thenApply(cluster -> checkEntityExists(cluster, "Cluster %s cannot be found.", clusterId))
        .thenCompose(
            cluster ->
                KafkaFutures.toCompletableFuture(
                    adminClient.deleteTopics(singletonList(topicName)).all()));
  }

  @Override
  public CompletableFuture<Void> updateTopicPartitionsCount(
      String topicName, Integer partitionsCount) {
    Map<String, NewPartitions> newPartitionsMap =
        Collections.singletonMap(topicName, NewPartitions.increaseTo(partitionsCount));
    return KafkaFutures.toCompletableFuture(adminClient.createPartitions(newPartitionsMap).all());
  }
}
