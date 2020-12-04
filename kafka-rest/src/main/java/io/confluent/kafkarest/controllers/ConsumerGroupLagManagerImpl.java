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

import io.confluent.kafkarest.common.KafkaFutures;
import io.confluent.kafkarest.entities.ConsumerGroupLag;
import io.confluent.kafkarest.resources.ConsumerGroupOffsets;
import io.confluent.kafkarest.resources.ConsumerOffsetsDao;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.inject.Inject;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.IsolationLevel;

final class ConsumerGroupLagManagerImpl implements ConsumerGroupLagManager {

  private final Admin adminClient;
  private final ConsumerGroupManager consumerGroupManager;
//  private final ConsumerOffsetsDao consumerOffsetsDao;

  @Inject
  ConsumerGroupLagManagerImpl(Admin adminClient, ConsumerGroupManager consumerGroupManager) {
    this.adminClient = requireNonNull(adminClient);
    this.consumerGroupManager = requireNonNull(consumerGroupManager);
//    this.consumerOffsetsDao = new ConsumerOffsetsDao(clusterId, adminClient, (int) TimeUnit.SECONDS.toMillis(15));
  }

//  @Override
//  public CompletableFuture<Optional<ConsumerGroupLag>> getConsumerGroupLag(
//      String clusterId,
//      String consumerGroupId
//  ) {
//    return consumerGroupManager.getConsumerGroup(clusterId, consumerGroupId)
//        .thenApply(
//            consumerGroup ->
//                checkEntityExists(
//                    consumerGroup, "Consumer Group %s does not exist.", consumerGroupId))
//            .thenCompose();
//  }

  @Override
  public CompletableFuture<Optional<ConsumerGroupLag>> getConsumerGroupLag(
      String clusterId,
      String consumerGroupId
  ) {
    ConsumerOffsetsDao consumerOffsetsDao = new ConsumerOffsetsDao(clusterId, adminClient, (int) TimeUnit.SECONDS.toMillis(15));
    try {
      ConsumerGroupOffsets cgo = consumerOffsetsDao.getConsumerGroupOffsets(consumerGroupId, IsolationLevel.READ_COMMITTED);
      return CompletableFuture.completedFuture(Optional.ofNullable(ConsumerGroupLag.fromConsumerGroupOffsets(clusterId, cgo)));
//      return consumerGroupManager.getConsumerGroup(clusterId, consumerGroupId)
//          .thenApply(
//              consumerGroup ->
//                  checkEntityExists(
//                      consumerGroup, "Consumer Group %s does not exist.", consumerGroupId))
//          .thenCompose(KafkaFutures.toCompletableFuture(
//              consumerOffsetsDao.getConsumerGroupOffsets(consumerGroupId, IsolationLevel.READ_COMMITTED)
//              ));
    } catch (ArithmeticException e) {
      // log.warn("lag exceeds Integer.MAX_VALUE", e);

    } catch (Exception e) {
      // log.warn("unable to fetch offsets for consumer group", e);
    }
  }

}