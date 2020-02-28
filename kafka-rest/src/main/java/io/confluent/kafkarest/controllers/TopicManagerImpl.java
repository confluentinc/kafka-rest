package io.confluent.kafkarest.controllers;

import io.confluent.kafkarest.entities.Topic;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaFuture;

import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class TopicManagerImpl implements TopicManager {

    private final Admin adminClient;
    private final ClusterManager clusterManager;

    @Inject
    TopicManagerImpl(Admin adminClient, ClusterManager clusterManager) {
        this.adminClient = Objects.requireNonNull(adminClient);
        this.clusterManager = Objects.requireNonNull(clusterManager);
    }

    public CompletableFuture<List<Topic>> listTopics(String clusterId) {
        // check if clusterId is correct
        Objects.requireNonNull(clusterId);

        clusterManager.getCluster(clusterId);


        ListTopicsResult listTopicsResult = adminClient.
                listTopics(new ListTopicsOptions());


//        ListTopicResult listTopicResult = adminClient.
//                listTopics(new ListTopicsOptions());
//        return CompletableFuture.completedFuture(new Topic.Builder())
//                .thenCombine(
//                    toCompletableFuture(listTopicsResult.names()),
//                    (topicBuilder, topicNames) -> {
//                        topicNames.stream()
//                            .filter(topicName -> {
//                                if(topicName == null) {
//                                    return null;
//                                }
//                                new Topic.Builder().setTopicName(topicName);
//                            }).collect(Collectors.toList())
//                    })

    }

    public CompletableFuture<Optional<Topic>> getTopic(String clusterId, String topicName) {
        DescribeTopicsResult describeTopicResult =
                adminClient.describeTopics(new ArrayList<String>());

        Objects.requireNonNull(topicName);
        return listTopics().thenApply(
            topics ->
                topics.stream()
                .filter(t -> t.getName().equals(topicName))
                .findAny());
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
