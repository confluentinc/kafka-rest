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
import static java.util.Objects.requireNonNull;

import io.confluent.kafkarest.entities.ConsumerAssignment;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.inject.Inject;

final class ConsumerAssignmentManagerImpl implements ConsumerAssignmentManager {

  private final ConsumerManager consumerManager;

  @Inject
  ConsumerAssignmentManagerImpl(ConsumerManager consumerManager) {
    this.consumerManager = requireNonNull(consumerManager);
  }

  @Override
  public CompletableFuture<List<ConsumerAssignment>> listConsumerAssignments(
      String clusterId, String consumerGroupId, String consumerId) {
    return consumerManager.getConsumer(clusterId, consumerGroupId, consumerId)
        .thenApply(
            consumer ->
                checkEntityExists(consumer, "Consumer %s does not exist.", consumerId))
        .thenApply(
            consumer ->
                consumer.getAssignedPartitions()
                    .stream()
                    .map(
                        partition ->
                            ConsumerAssignment.builder()
                                .setClusterId(clusterId)
                                .setConsumerGroupId(consumerGroupId)
                                .setConsumerId(consumerId)
                                .setTopicName(partition.getTopicName())
                                .setPartitionId(partition.getPartitionId())
                                .build())
                    .collect(Collectors.toList()));
  }

  @Override
  public CompletableFuture<Optional<ConsumerAssignment>> getConsumerAssignment(
      String clusterId,
      String consumerGroupId,
      String consumerId,
      String topicName,
      int partitionId
  ) {
    return listConsumerAssignments(clusterId, consumerGroupId, consumerId)
        .thenApply(
            assignments ->
                assignments.stream()
                    .filter(assignment -> assignment.getTopicName().equals(topicName))
                    .filter(assignment -> assignment.getPartitionId() == partitionId)
                    .findAny());
  }
}
