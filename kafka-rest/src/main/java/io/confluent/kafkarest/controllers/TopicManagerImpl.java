/*
 * Copyright 2018 Confluent Inc.
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

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

import io.confluent.kafkarest.entities.Cluster;
import io.confluent.kafkarest.entities.Partition;
import io.confluent.kafkarest.entities.PartitionReplica;
import io.confluent.kafkarest.entities.Topic;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

public class TopicManagerImpl implements TopicManager {

  private final Admin adminClient;
  private final ClusterManager clusterManager;
  private String clusterId;

  @Inject
  TopicManagerImpl(Admin adminClient, ClusterManager clusterManager) {
    this.adminClient = Objects.requireNonNull(adminClient);
    this.clusterManager = Objects.requireNonNull(clusterManager);
  }

  @Override
  public CompletableFuture<List<Topic>> listTopics(String clusterId) {
    Objects.requireNonNull(clusterId);

    CompletableFuture<Optional<Cluster>> completableCluster = clusterManager.getCluster(clusterId);

    return completableCluster.thenCompose(
        clusterIdOpt -> {
          if (!clusterIdOpt.isPresent()) {
            return CompletableFuture.completedFuture(emptyList());
          }
          return toCompletableFuture(adminClient.listTopics().listings())
              .thenCompose(listing -> {
                if (listing == null) {
                  return CompletableFuture.completedFuture(emptyList());
                }
                return describeTopics(
                    listing.stream().map(TopicListing::name).collect(Collectors.toList()),
                    clusterId);
              });
        });
  }

  public CompletableFuture<Optional<Topic>> getTopic(String clusterId, String topicName) {
    Objects.requireNonNull(clusterId);
    Objects.requireNonNull(topicName);

    CompletableFuture<Optional<Cluster>> completableCluster = clusterManager.getCluster(clusterId);

    return completableCluster.thenCompose(
        clusterIdOpt -> {
          if (!clusterIdOpt.isPresent()) {
            return CompletableFuture.completedFuture(Optional.empty());
          }
          return describeTopics(singletonList(topicName), clusterId)
              .thenApply(
                  topics -> {
                    if (topics.isEmpty()) {
                      return Optional.empty();
                    }
                    return Optional.of(topics.get(0));
                  });
        });
  }

  private CompletableFuture<List<Topic>> describeTopics(List<String> topicNames, String clusterId) {
    this.clusterId = clusterId;
    DescribeTopicsResult describeTopicsResult = adminClient.describeTopics(topicNames);
    return CompletableFuture.completedFuture(new ArrayList<Topic>())
        .thenCombine(
            toCompletableFuture(describeTopicsResult.all()),
            (topics, describeTopicsResultMap) -> {
              if (describeTopicsResultMap == null) {
                return null;
              }
              return describeTopicsResultMap.values().stream().map(this::toTopic)
                  .collect(Collectors.toList());
            });
  }

  public Topic toTopic(TopicDescription topicDescription) {
    return new Topic.Builder()
        .setTopicName(topicDescription.name())
        .setIsInternal(topicDescription.isInternal())
        .setReplicationFactor(topicDescription.partitions().get(0).replicas().size())
        .setPartitions(getPartitions(topicDescription))
        .setConfigs(new Properties())
        .setClusterId(clusterId)
        .build();
  }

  private <T> CompletableFuture<T> toCompletableFuture(KafkaFuture<T> kafkaFuture) {
    CompletableFuture<T> completableFuture = new CompletableFuture<>();
    kafkaFuture.whenComplete(
        (value, exception) -> {
          if (exception == null) {
            completableFuture.complete(value);
          } else {
            completableFuture.completeExceptionally(exception);
          }
        });
    return completableFuture;
  }

  private static List<Partition> getPartitions(TopicDescription description) {
    List<Partition> partitions = new ArrayList<>();
    List<TopicPartitionInfo> topicPartitionInfos = description.partitions();
    for (TopicPartitionInfo topicPartitionInfo : topicPartitionInfos) {
      Set<Node> isr = new HashSet(topicPartitionInfo.isr());
      List<PartitionReplica> partitionReplicas = new ArrayList<>();
      List<Node> replicas = topicPartitionInfo.replicas();
      for (Node replica : replicas) {
        PartitionReplica partitionReplica = new PartitionReplica(replica.id(),
            topicPartitionInfo.leader().equals(replica), isr.contains(replica));
        partitionReplicas.add(partitionReplica);
      }
      Partition partition = new Partition(topicPartitionInfo.partition(),
          topicPartitionInfo.leader().id(), partitionReplicas);
      partitions.add(partition);
    }
    return partitions;
  }
}
