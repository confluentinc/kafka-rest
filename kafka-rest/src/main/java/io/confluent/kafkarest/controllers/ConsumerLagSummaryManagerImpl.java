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

import io.confluent.kafkarest.entities.ConsumerLagSummary;
import io.confluent.kafkarest.entities.ConsumerGroup;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import javax.inject.Inject;

final class ConsumerLagSummaryManagerImpl implements ConsumerLagSummaryManager {

  private final ConsumerGroupManager consumerGroupManager;

  @Inject
  ConsumerLagSummaryManagerImpl(ConsumerGroupManager consumerGroupManager) {
    this.consumerGroupManager = consumerGroupManager;
  }

  @Override
  public CompletableFuture<Optional<ConsumerLagSummary>> getConsumerLagSummary(
      String clusterId,
      String consumerGroupId
  ) {
    return consumerGroupManager.getConsumerGroup(clusterId, consumerGroupId)
        .thenApply(
            consumerGroup ->
                checkEntityExists(
                    consumerGroup, "Consumer Group %s does not exist.", consumerGroupId))
            .thenCompose();
  }

}