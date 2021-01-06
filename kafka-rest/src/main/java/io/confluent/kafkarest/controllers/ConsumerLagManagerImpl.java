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

import static java.util.Objects.requireNonNull;

import io.confluent.kafkarest.entities.ConsumerLag;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import javax.inject.Inject;
import org.apache.kafka.common.IsolationLevel;

final class ConsumerLagManagerImpl implements ConsumerLagManager {

  private final ConsumerOffsetsDao consumerOffsetsDao;

  @Inject
  ConsumerLagManagerImpl(ConsumerOffsetsDao consumerOffsetsDao) {
    this.consumerOffsetsDao = requireNonNull(consumerOffsetsDao);
  }

  @Override
  public CompletableFuture<List<ConsumerLag>> listConsumerLags(
      String clusterId, String consumerGroupId) {
    try {
      return CompletableFuture.completedFuture(
          consumerOffsetsDao.getConsumerLags(clusterId, consumerGroupId, IsolationLevel.READ_COMMITTED));
    } catch (Exception e) {
      return CompletableFuture.completedFuture(new ArrayList<>());
    }
  }

  @Override
  public CompletableFuture<Optional<ConsumerLag>> getConsumerLag(
      String clusterId, String topicName, Integer partitionId, String consumerGroupId) {
    return listConsumerLags(clusterId, consumerGroupId)
        .thenApply(
            lags ->
                lags.stream()
                    .filter(lag -> lag.getTopicName().equals(topicName))
                    .filter(lag -> lag.getPartitionId() == partitionId)
                    .filter(lag -> lag.getConsumerGroupId().equals(consumerGroupId))
                    .findAny());
  }
}
