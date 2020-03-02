package io.confluent.kafkarest.controllers;

import io.confluent.kafkarest.entities.Cluster;
import io.confluent.kafkarest.entities.Partition;
import io.confluent.kafkarest.entities.Topic;
import io.confluent.kafkarest.entities.TopicsMap;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaFuture;

import javax.inject.Inject;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

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
    public CompletableFuture<TopicsMap> listTopics(String clusterId) {
        Objects.requireNonNull(clusterId);
        //todo check if clusterId is correct this way is sufficient
        CompletableFuture<Optional<Cluster>> futureCluster = clusterManager.getCluster(clusterId);
        Objects.requireNonNull(futureCluster);

        ListTopicsResult listTopicsResult = adminClient.
                listTopics(new ListTopicsOptions());

        return CompletableFuture.completedFuture(new TopicsMap.Builder())
            .thenCombine(
                toCompletableFuture(listTopicsResult.namesToListings()),
                (topicsMapBuilder, namesToListings) -> {
                    if (namesToListings == null) {
                        return null;
                    }
                    return topicsMapBuilder.setTopicsMap(namesToListings);
                })
            .thenApply(
                topicsMapBuilder -> {
                    if (topicsMapBuilder == null) {
                        return null;
                    }
                    return topicsMapBuilder.build();
                });
    }

    public CompletableFuture<List<Topic>> getTopic(String clusterId, String topicName) {
        // check if clusterId is correct
        Objects.requireNonNull(clusterId);
        CompletableFuture<Optional<Cluster>> futureCluster = clusterManager.getCluster(clusterId);
        Objects.requireNonNull(futureCluster);

        DescribeTopicsResult describeTopicResult =
                adminClient.describeTopics(Collections.unmodifiableList(Arrays.asList(topicName)));

        return CompletableFuture.completedFuture(new Topic.Builder())
            .thenCombine(
                    toCompletableFuture(describeTopicResult.values().get(topicName)),
                    (topicBuilder, description) -> {
                        if (description == null) {
                            return null;
                        }
                        return topicBuilder.setTopicName(topicName);
                    })
            .thenCombine(
                    toCompletableFuture(describeTopicResult.values().get(topicName)),
                    (topicBuilder, description) -> {
                        if(topicBuilder == null) {
                            return null;
                        }
                        if(description.partitions() == null) {
                            return topicBuilder;
                        }
                        // todo needs some processing since partitions in description are TopicPartitionsInfo
                        return topicBuilder.setPartitions(
                                description.partitions().stream()
                                .filter(topicPartitionInfo -> topicPartitionInfo != null)
                                .map(Partition::new)
                                .collect(Collectors.toList()));
                    })
            .thenCombine(
                    toCompletableFuture(describeTopicResult.values().get(topicName)),
                    (topicBuilder, description) -> {
                        if(topicBuilder == null) {
                            return null;
                        }
                        return topicBuilder.setIsInternal(description.isInternal());
                    })
            // todo need to derive and set the replication count
            // todo configs to be left empty for now
            .thenApply(
                    topicBuilder -> {
                        if(topicBuilder == null) {
                            return emptyList();
                        }
                        return unmodifiableList(singletonList(topicBuilder.build()));
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
