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

import io.confluent.kafkarest.controllers.ConsumerOffsetsDaoImpl.MemberId;
import io.confluent.kafkarest.entities.ConsumerGroupLag;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import javax.inject.Inject;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.TopicPartition;

final class ConsumerGroupLagManagerImpl implements ConsumerGroupLagManager {

  private final ConsumerOffsetsDao consumerOffsetsDao;
  private final IsolationLevel isolationLevel = IsolationLevel.READ_COMMITTED;

  @Inject
  ConsumerGroupLagManagerImpl(
      ConsumerOffsetsDao consumerOffsetsDao) {
    this.consumerOffsetsDao = requireNonNull(consumerOffsetsDao);
  }

  @Override
  public CompletableFuture<Optional<ConsumerGroupLag>> getConsumerGroupLag(
      String clusterId,
      String consumerGroupId
  ) {
    CompletableFuture<ConsumerGroupDescription> cgDesc =
        consumerOffsetsDao.getConsumerGroupDescription(consumerGroupId);
    CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> fetchedCurrentOffsets =
        consumerOffsetsDao.getCurrentOffsets(consumerGroupId);
    CompletableFuture<Map<TopicPartition, ListOffsetsResultInfo>> latestOffsets =
        consumerOffsetsDao.getLatestOffsets(isolationLevel, fetchedCurrentOffsets);
    CompletableFuture<Map<TopicPartition, MemberId>> tpMemberIds =
        consumerOffsetsDao.getMemberIds(cgDesc);
  }

//  @Override
//  public CompletableFuture<Optional<ConsumerGroupLag>> getConsumerGroupLag(
//      String clusterId,
//      String consumerGroupId
//  ) {
//    try {
//      ConsumerGroupLag lag = consumerOffsetsDao
//          .getConsumerGroupOffsets(clusterId, consumerGroupId, IsolationLevel.READ_COMMITTED);
//      return CompletableFuture.completedFuture(Optional.ofNullable(lag));
//    } catch (Exception e) {
//      // log.warn("unable to fetch offsets for consumer group", e);
//      return CompletableFuture.completedFuture(Optional.empty());
//    }
//  }
}