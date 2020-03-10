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

import io.confluent.kafkarest.entities.Partition;
import io.confluent.kafkarest.entities.Topic;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.ws.rs.NotFoundException;

class PartitionManagerImpl implements PartitionManager {

  private final TopicManager topicManager;

  @Inject
  PartitionManagerImpl(TopicManager topicManager) {
    this.topicManager = Objects.requireNonNull(topicManager);
  }

  @Override
  public CompletableFuture<List<Partition>> listPartitions(String clusterId, String topicName) {
    return topicManager.getTopic(clusterId, topicName)
        .thenApply(
            topic ->
                topic.orElseThrow(
                    () -> new NotFoundException(
                        String.format("Topic %s cannot be found.", topicName))))
        .thenApply(Topic::getPartitions);
  }

  @Override
  public CompletableFuture<Optional<Partition>> getPartition(
      String clusterId, String topicName, int partitionId) {
    return listPartitions(clusterId, topicName)
        .thenApply(
            partitions ->
                partitions.stream()
                    .filter(partition -> partition.getPartitionId() == partitionId)
                    .collect(Collectors.toList()))
        .thenApply(partitions -> partitions.stream().findAny());
  }
}
