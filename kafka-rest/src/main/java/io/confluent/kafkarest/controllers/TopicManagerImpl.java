package io.confluent.kafkarest.controllers;

import io.confluent.kafkarest.entities.Cluster;
import io.confluent.kafkarest.entities.Partition;
import io.confluent.kafkarest.entities.PartitionReplica;
import io.confluent.kafkarest.entities.Topic;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;

import static java.util.Collections.unmodifiableList;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import static java.util.Collections.*;

public class TopicManagerImpl implements TopicManager {

  private final Admin adminClient;
  private final ClusterManager clusterManager;

  @Inject
  TopicManagerImpl(Admin adminClient, ClusterManager clusterManager) {
    this.adminClient = Objects.requireNonNull(adminClient);
    this.clusterManager = Objects.requireNonNull(clusterManager);
  }

  @Override
  public CompletableFuture<List<Topic>> listTopics(String clusterId) {
    Objects.requireNonNull(clusterId);
    // todo check if clusterId is correct with clustermanager
//        CompletableFuture<Optional<Cluster>> c = clusterManager.listClusters().thenApply(
//                clusters ->
//                        clusters.stream()
//                                .filter(cluster -> cluster.getClusterId().equals(clusterId))
//                                .findAny());

    ListTopicsResult listTopicsResult = adminClient.listTopics();

    return CompletableFuture.completedFuture(new ArrayList<Topic.Builder>())
      .thenCombine(
        toCompletableFuture(listTopicsResult.namesToListings()),
        (topicBuilderList, topicsMap) -> {
          if (topicsMap == null) {
            return null;
          }
          topicsMap.forEach((topic, topicListing) -> {
            Topic.Builder topicBuilder = new Topic.Builder();
            topicBuilder.setTopicName(topic);
            topicBuilder.setIsInternal(topicListing.isInternal());
            topicBuilder.setConfigs(new Properties());
            // for now
            topicBuilder.setPartitions(new ArrayList<Partition>(
              Collections.singleton(new Partition(0, 1, null))));
            // todo we can add clusterid as a attribute to topics
            // topicBuilder.setClusterId(clusterId);
            topicBuilderList.add(topicBuilder);
          });
          return topicBuilderList;
        })
      .thenApply(
        topicBuilderList -> {
          if (topicBuilderList == null) {
            return null;
          }
          // topicInfoBuilder.setClusterId(clusterId);
          List<Topic> topicsList = new ArrayList<>();
          for (Topic.Builder topicBuilder : topicBuilderList) {
            topicsList.add(topicBuilder.build());
          }
          return topicsList;
        });
  }

  public CompletableFuture<Topic> getTopic(String clusterId, String topicName) {
    // check if clusterId is correct
    Objects.requireNonNull(clusterId);

    CompletableFuture<Optional<Cluster>> futureCluster = clusterManager.getCluster(clusterId);
    Objects.requireNonNull(futureCluster);

    DescribeTopicsResult describeTopicResult =
      adminClient.describeTopics(Collections.unmodifiableList(Arrays.asList(topicName)));

    /**
     * topicName
     * replicationFactor
     * isInternal
     * partitions
     */
    return CompletableFuture.completedFuture(new Topic.Builder())
      .thenCombine(
        toCompletableFuture(describeTopicResult.values().get(topicName)),
        (topicBuilder, description) -> {
          if (topicBuilder == null) {
            return null;
          }
          if (description == null) {
            return topicBuilder;
          }
          return topicBuilder.setTopicName(topicName);
        })
      .thenCombine(
        toCompletableFuture(describeTopicResult.values().get(topicName)),
        (topicBuilder, description) -> {
          if (topicBuilder == null) {
            return null;
          }
          if (description == null) {
            return topicBuilder;
          }
          return topicBuilder.setReplicationFactor(description.partitions().get(0).replicas().size());
        })
      .thenCombine(
        toCompletableFuture(describeTopicResult.values().get(topicName)),
        (topicBuilder, description) -> {
          if (topicBuilder == null) {
            return null;
          }
          if (description.partitions() == null) {
            return topicBuilder;
          }
          // todo needs some processing since partitions in description are TopicPartitionsInfo
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
          return topicBuilder.setPartitions(partitions);
//                        alternate functional
//                        description.partitions().stream().filter(topicPartitionInfo -> {
//                            if (topicPartitionInfo != null) {
//                                Set<Node> isr = new HashSet(topicPartitionInfo.isr());
//                                List<PartitionReplica> partitionReplicas = new ArrayList<>();
//                                topicPartitionInfo.replicas().stream().filter(replica -> {
//                                    PartitionReplica partitionReplica = new PartitionReplica(replica.id(),
//                                                                            topicPartitionInfo.leader().equals(replica),
//                                                                            isr.contains(replica));
//                                    partitionReplicas.add(partitionReplica);
//                                });
//                                Partition partition = new Partition(topicPartitionInfo.partition(),
//                                       topicPartitionInfo.leader().id(), partitionReplicas);
//                                partitions.add(partition);
//                            }
        })
      .thenCombine(
        toCompletableFuture(describeTopicResult.values().get(topicName)),
        (topicBuilder, description) -> {
          if (topicBuilder == null) {
            return null;
          }
          if (description == null) {
            return topicBuilder;
          }
          return topicBuilder.setIsInternal(description.isInternal());
        })
      // configs to be left empty for now
      .thenApply(
        topicBuilder -> {
          if (topicBuilder == null) {
            return null;
          }
          return topicBuilder.build();
        });
  }

  private static <T> CompletableFuture<T> toCompletableFuture(KafkaFuture<T> kafkaFuture) {
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
}
