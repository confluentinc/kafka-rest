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

import io.confluent.kafkarest.entities.Consumer;
import io.confluent.kafkarest.entities.ConsumerGroup;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import javax.inject.Inject;

final class ConsumerManagerImpl implements ConsumerManager {

  private final ConsumerGroupManager consumerGroupManager;

  @Inject
  ConsumerManagerImpl(ConsumerGroupManager consumerGroupManager) {
    this.consumerGroupManager = consumerGroupManager;
  }

  @Override
  public CompletableFuture<List<Consumer>> listConsumers(String clusterId, String consumerGroupId) {
    return consumerGroupManager.getConsumerGroup(clusterId, consumerGroupId)
        .thenApply(
            consumerGroup ->
                checkEntityExists(
                    consumerGroup, "Consumer Group %s does not exist.", consumerGroupId))
        .thenApply(ConsumerGroup::getConsumers);
  }

  @Override
  public CompletableFuture<Optional<Consumer>> getConsumer(
      String clusterId, String consumerGroupId, String consumerId) {
    return listConsumers(clusterId, consumerGroupId)
        .thenApply(
            consumers -> findEntityByKey(consumers, Consumer::getConsumerId, consumerId));
  }
}
