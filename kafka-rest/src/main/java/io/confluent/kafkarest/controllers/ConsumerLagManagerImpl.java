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

import io.confluent.kafkarest.entities.ConsumerLag;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import javax.inject.Inject;

final class ConsumerLagManagerImpl implements ConsumerLagManager {

  private final ConsumerGroupManager consumerGroupManager;

  @Inject
  ConsumerLagManagerImpl(ConsumerGroupManager consumerGroupManager) {
    this.consumerGroupManager = requireNonNull(consumerGroupManager);
  }

  @Override
  public CompletableFuture<List<ConsumerLag>> listConsumerLags(
      String clusterId, String consumerGroupId) {
    return consumerGroupManager.getConsumerGroup(clusterId, consumerGroupId)
        .thenApply(
            consumerGroup ->
                checkEntityExists(consumerGroup, "Consumer group %s does not exist.",
                    consumerGroupId))
        .thenApply(
            consumerGroup ->
                null);
  }

  @Override
  public CompletableFuture<Optional<ConsumerLag>> getConsumerLag(
      String clusterId, String consumerGroupId, String consumerId) {
    return listConsumerLags(clusterId, consumerGroupId)
        .thenApply(
            lags ->
                lags.stream()
                    .filter(lag -> lag.getConsumerId().equals(consumerId))
                    .findAny());
  }
}
