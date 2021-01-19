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
import io.confluent.kafkarest.entities.ConsumerLag;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import javax.inject.Inject;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.TopicPartition;

final class ConsumerLagManagerImpl implements ConsumerLagManager {

  private final ConsumerOffsetsDao consumerOffsetsDao;
  private final IsolationLevel isolationLevel = IsolationLevel.READ_COMMITTED;

  @Inject
  ConsumerLagManagerImpl(ConsumerOffsetsDao consumerOffsetsDao) {
    this.consumerOffsetsDao = requireNonNull(consumerOffsetsDao);
  }

  @Override
  public CompletableFuture<List<ConsumerLag>> listConsumerLags(
      String clusterId, String consumerGroupId) {
    CompletableFuture<ConsumerGroupDescription> cgDesc =
        consumerOffsetsDao.getConsumerGroupDescription(consumerGroupId);
    CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> fetchedCurrentOffsets =
        consumerOffsetsDao.getCurrentOffsets(consumerGroupId);
    CompletableFuture<Map<TopicPartition, ListOffsetsResultInfo>> latestOffsets =
        consumerOffsetsDao.getLatestOffsets(isolationLevel, fetchedCurrentOffsets);
    CompletableFuture<Map<TopicPartition, MemberId>> tpMemberIds =
        consumerOffsetsDao.getMemberIds(cgDesc);

    CompletableFuture<List<ConsumerLag>> consumerLags =
        CompletableFuture.supplyAsync(ArrayList::new);

    return CompletableFuture.allOf(
        fetchedCurrentOffsets, latestOffsets, tpMemberIds, consumerLags
    ).thenApply(x -> {
      final Map<TopicPartition, OffsetAndMetadata> fetchedCurrentOffsetsJoined = fetchedCurrentOffsets.join();
      final Map<TopicPartition, ListOffsetsResultInfo> latestOffsetsJoined = latestOffsets.join();
      final List<ConsumerLag> consumerLagsJoined = consumerLags.join();
      fetchedCurrentOffsetsJoined.keySet().stream()
          .forEach(
              tp -> {
                MemberId memberId = tpMemberIds.join().getOrDefault(
                    tp, MemberId.builder()
                        .setConsumerId("")
                        .setClientId("")
                        .setInstanceId(Optional.empty())
                        .build());
                long currentOffset = consumerOffsetsDao.getCurrentOffset(fetchedCurrentOffsetsJoined, tp);
                long latestOffset = consumerOffsetsDao.getOffset(latestOffsetsJoined, tp);
                // ahu todo: ask about log.debug
                consumerLagsJoined.add(
                    ConsumerLag.builder()
                        .setClusterId(clusterId)
                        .setConsumerGroupId(consumerGroupId)
                        .setTopicName(tp.topic())
                        .setPartitionId(tp.partition())
                        .setConsumerId(memberId.getConsumerId())
                        .setInstanceId(memberId.getInstanceId().orElse(null))
                        .setClientId(memberId.getClientId())
                        .setCurrentOffset(currentOffset)
                        .setLogEndOffset(latestOffset)
                        .build());
              });
      return consumerLagsJoined;
    });
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
