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

import io.confluent.kafkarest.entities.Partition;
import io.confluent.kafkarest.entities.Topic;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import javax.inject.Inject;

class PartitionManagerImpl implements PartitionManager {

  private final TopicManager topicManager;

  @Inject
  PartitionManagerImpl(TopicManager topicManager) {
    this.topicManager = Objects.requireNonNull(topicManager);
  }

  @Override
  public CompletableFuture<List<Partition>> listPartitions(String clusterId, String topicName) {
    return topicManager.getTopic(clusterId, topicName)
        .thenApply(topic -> checkEntityExists(topic, "Topic %s cannot be found.", topic))
        .thenApply(Topic::getPartitions);
  }

  @Override
  public CompletableFuture<Optional<Partition>> getPartition(
      String clusterId, String topicName, int partitionId) {
    return listPartitions(clusterId, topicName)
        .thenApply(
            partitions -> findEntityByKey(partitions, Partition::getPartitionId, partitionId));
  }
}
