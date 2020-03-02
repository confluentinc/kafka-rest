package io.confluent.kafkarest.controllers;

import io.confluent.kafkarest.entities.Cluster;
import io.confluent.kafkarest.entities.Topic;
import io.confluent.kafkarest.entities.TopicInfo;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaFuture;
import static java.util.Collections.unmodifiableList;

import javax.inject.Inject;
import java.util.ArrayList;
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
    public CompletableFuture<List<Topic>> listTopics(String clusterId) {
        Objects.requireNonNull(clusterId);
        //todo check if clusterId is correct this way is sufficient
        CompletableFuture<Optional<Cluster>> futureCluster = clusterManager.getCluster(clusterId);
        Objects.requireNonNull(futureCluster);

        ListTopicsResult listTopicsResult = adminClient.
                listTopics(new ListTopicsOptions());

//        return CompletableFuture.completedFuture(new TopicInfo.Builder())
//            .thenCombine(
//                toCompletableFuture(listTopicsResult.listings()),
//                (topicInfoBuilder, topicListings) -> {
//                    if (topicListings == null) {
//                        return null;
//                    }
//                    return topicInfoBuilder.setTopicListings((List<TopicListing>) topicListings);
//                })
//            .thenApply(
//                topicInfoBuilder -> {
//                    if (topicInfoBuilder == null) {
//                        return null;
//                    }
//                    topicInfoBuilder.setClusterId(clusterId);
//                    return topicInfoBuilder.build();
//                });
        return CompletableFuture.completedFuture(new ArrayList<Topic.Builder>())
                .thenCombine(
                        toCompletableFuture(listTopicsResult.namesToListings()),
                        (topicBuilderList, topicsMap) -> {
                            if (topicsMap == null) {
                                return null;
                            }
                            topicsMap.forEach((k, v) -> {
                                Topic.Builder topicBuilder = new Topic.Builder();
                                topicBuilder.setTopicName(k);
                                topicBuilder.setIsInternal(v.isInternal());
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
                            for(Topic.Builder topicBuilder : topicBuilderList) {
                                topicsList.add(topicBuilder.build());
                            }
                            return topicsList;
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
