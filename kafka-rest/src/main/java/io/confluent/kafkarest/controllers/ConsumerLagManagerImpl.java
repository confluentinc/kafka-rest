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

import io.confluent.kafkarest.common.KafkaFutures;
import io.confluent.kafkarest.controllers.ConsumerOffsetsDaoImpl.MemberId;
import io.confluent.kafkarest.entities.ConsumerLag;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.ws.rs.NotFoundException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsOptions;
import org.apache.kafka.clients.admin.ListOffsetsOptions;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.ConsumerGroupState;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.TopicPartition;

final class ConsumerLagManagerImpl implements ConsumerLagManager {

  private final Admin adminClient;
  private final ConsumerOffsetsDao consumerOffsetsDao;
  private final IsolationLevel isolationLevel = IsolationLevel.READ_COMMITTED;

  @Inject
  ConsumerLagManagerImpl(ConsumerOffsetsDao consumerOffsetsDao) {
//    this.adminClient = requireNonNull(adminClient);
    this.consumerOffsetsDao = requireNonNull(consumerOffsetsDao);
    this.adminClient = consumerOffsetsDao.getKafkaAdminClient();
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


//    CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> fetchedCurrentOffsets =
//        KafkaFutures.toCompletableFuture(
//            adminClient.listConsumerGroupOffsets(
//                consumerGroupId, new ListConsumerGroupOffsetsOptions())
//                .partitionsToOffsetAndMetadata());
//
//    CompletableFuture<Map<TopicPartition, OffsetSpec>> latestOffsetSpecs =
//        fetchedCurrentOffsets
//            .thenApply(
//                map ->
//                    map.keySet().stream()
//                        .collect(Collectors.toMap(Function.identity(), tp -> OffsetSpec.latest())));
//    CompletableFuture<Map<TopicPartition, ListOffsetsResultInfo>> latestOffsets =
//        latestOffsetSpecs
//            .thenApply(
//                specs ->
//                    adminClient.listOffsets(specs, new ListOffsetsOptions(isolationLevel)))
//            .thenCompose(result ->
//                KafkaFutures.toCompletableFuture(result.all()));
//
//    CompletableFuture<Map<TopicPartition, MemberId>> tpMemberIds =
//        CompletableFuture.supplyAsync(HashMap::new);
//
//    cgDesc.thenCombine(
//        tpMemberIds, (desc, memberIds) -> {
//            desc.members().stream()
//                .forEach(
//                    memberDesc ->
//                        memberDesc.assignment().topicPartitions()
//                            .stream()
//                            .forEach(
//                                tp ->
//                                    memberIds.put(
//                                        tp, MemberId.builder()
//                                            .setConsumerId(memberDesc.consumerId())
//                                            .setClientId(memberDesc.clientId())
//                                            .setInstanceId(memberDesc.groupInstanceId())
//                                            .build())));
//            return null; });

    CompletableFuture<List<ConsumerLag>> consumerLags =
        CompletableFuture.supplyAsync(ArrayList::new);

    CompletableFuture<List<ConsumerLag>> result = CompletableFuture.allOf(
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
                long currentOffset = getCurrentOffset(fetchedCurrentOffsetsJoined, tp);
                long latestOffset = getOffset(latestOffsetsJoined, tp);
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
    return result;
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

  private long getCurrentOffset(Map<TopicPartition, OffsetAndMetadata> map, TopicPartition tp) {
    if (map == null) {
      return -1;
    }
    OffsetAndMetadata oam = map.get(tp);
    if (oam == null) {
      return -1;
    }
    return oam.offset();
  }

  private long getOffset(Map<TopicPartition, ListOffsetsResultInfo> map, TopicPartition tp) {
    if (map == null) {
      return -1;
    }

    ListOffsetsResultInfo offsetInfo = map.get(tp);
    if (offsetInfo == null) {
      return -1;
    }

    return offsetInfo.offset();
  }
}
