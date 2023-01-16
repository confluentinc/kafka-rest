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
import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;

import io.confluent.kafkarest.common.CompletableFutures;
import io.confluent.kafkarest.common.KafkaFutures;
import io.confluent.kafkarest.entities.Partition;
import io.confluent.kafkarest.entities.Topic;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import javax.inject.Inject;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ListOffsetsOptions;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.common.TopicPartition;

final class PartitionManagerImpl implements PartitionManager {

  private final Admin adminClient;
  private final TopicManager topicManager;

  @Inject
  PartitionManagerImpl(Admin adminClient, TopicManager topicManager) {
    this.adminClient = requireNonNull(adminClient);
    this.topicManager = requireNonNull(topicManager);
  }

  @Override
  public CompletableFuture<List<Partition>> listPartitions(String clusterId, String topicName) {
    return topicManager.getTopic(clusterId, topicName)
        .thenApply(topic -> checkEntityExists(topic, "Topic %s cannot be found.", topic))
        .thenApply(Topic::getPartitions)
        .thenCompose(this::withOffsets);
  }

  @Override
  public CompletableFuture<List<Partition>> listLocalPartitions(String topicName) {
    return topicManager.getLocalTopic(topicName)
        .thenApply(topic -> checkEntityExists(topic, "Topic %s cannot be found.", topic))
        .thenApply(Topic::getPartitions)
        .thenCompose(this::withOffsets);
  }

  @Override
  public CompletableFuture<Optional<Partition>> getPartition(
      String clusterId, String topicName, int partitionId) {
    return topicManager.getTopic(clusterId, topicName)
        .thenApply(topic -> checkEntityExists(topic, "Topic %s cannot be found.", topic))
        .thenApply(Topic::getPartitions)
        .thenApply(
            partitions -> findEntityByKey(partitions, Partition::getPartitionId, partitionId))
        .thenApply(
            partition -> partition.map(Collections::singletonList).orElse(emptyList()))
        .thenCompose(this::withOffsets)
        .thenApply(partitions -> partitions.stream().findAny());
  }

  @Override
  public CompletableFuture<Optional<Partition>> getLocalPartition(
      String topicName, int partitionId) {
    return topicManager.getLocalTopic(topicName)
        .thenApply(topic -> checkEntityExists(topic, "Topic %s cannot be found.", topic))
        .thenApply(Topic::getPartitions)
        .thenApply(
            partitions -> findEntityByKey(partitions, Partition::getPartitionId, partitionId))
        .thenApply(
            partition -> partition.map(Collections::singletonList).orElse(emptyList()))
        .thenCompose(this::withOffsets)
        .thenApply(partitions -> partitions.stream().findAny());
  }

  private CompletableFuture<List<Partition>> withOffsets(List<Partition> partitions) {
    if (partitions.isEmpty()) {
      return completedFuture(emptyList());
    }

    ListOffsetsResult earliestResponse = listOffsets(partitions, OffsetSpec.earliest());
    ListOffsetsResult latestResponse = listOffsets(partitions, OffsetSpec.latest());

    List<CompletableFuture<Partition>> partitionsWithOffsets = new ArrayList<>();
    for (Partition partition : partitions) {
      CompletableFuture<ListOffsetsResultInfo> earliestFuture =
          KafkaFutures.toCompletableFuture(
              earliestResponse.partitionResult(toTopicPartition(partition)));
      CompletableFuture<ListOffsetsResultInfo> latestFuture =
          KafkaFutures.toCompletableFuture(
              latestResponse.partitionResult(toTopicPartition(partition)));

      CompletableFuture<Partition> partitionWithOffset =
          earliestFuture.thenCombine(
              latestFuture,
              (earliest, latest) ->
                  Partition.create(
                      partition.getClusterId(),
                      partition.getTopicName(),
                      partition.getPartitionId(),
                      partition.getReplicas(),
                      earliest.offset(),
                      latest.offset()));

      partitionsWithOffsets.add(partitionWithOffset);
    }

    return CompletableFutures.allAsList(partitionsWithOffsets);
  }

  private ListOffsetsResult listOffsets(List<Partition> partitions, OffsetSpec offsetSpec) {
    HashMap<TopicPartition, OffsetSpec> request = new HashMap<>();
    for (Partition partition : partitions) {
      request.put(toTopicPartition(partition), offsetSpec);
    }
    return adminClient.listOffsets(request, new ListOffsetsOptions());
  }

  private static TopicPartition toTopicPartition(Partition partition) {
    return new TopicPartition(partition.getTopicName(), partition.getPartitionId());
  }
}
