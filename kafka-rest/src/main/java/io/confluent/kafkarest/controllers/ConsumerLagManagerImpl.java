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
import io.confluent.kafkarest.entities.ConsumerLag;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.inject.Inject;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsOptions;
import org.apache.kafka.clients.admin.ListOffsetsOptions;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.TopicPartition;

final class ConsumerLagManagerImpl implements ConsumerLagManager {

  private final Admin adminClient;
  private final ConsumerOffsetsDao consumerOffsetsDao;
  private final IsolationLevel isolationLevel = IsolationLevel.READ_COMMITTED;

  @Inject
  ConsumerLagManagerImpl(Admin adminClient, ConsumerOffsetsDao consumerOffsetsDao) {
    this.adminClient = requireNonNull(adminClient);
    this.consumerOffsetsDao = requireNonNull(consumerOffsetsDao);
  }

  @Override
  public CompletableFuture<List<ConsumerLag>> listConsumerLags(
      String clusterId, String consumerGroupId) {
    try {
      return CompletableFuture.completedFuture(
          consumerOffsetsDao.getConsumerLags(
              clusterId, consumerGroupId, IsolationLevel.READ_COMMITTED));
    } catch (Exception e) {
      return CompletableFuture.completedFuture(new ArrayList<>());
    }
  }

  @Override
  public CompletableFuture<Optional<ConsumerLag>> getConsumerLag(
      String clusterId, String topicName, Integer partitionId, String consumerGroupId) {
    CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> fetchedCurrentOffsets =
        CompletableFuture.supplyAsync(
            ListConsumerGroupOffsetsOptions::new)
            .thenApply(
                options ->
                    adminClient.listConsumerGroupOffsets(consumerGroupId, options))
            .thenCompose(
                offsets ->
                    KafkaFutures.toCompletableFuture(offsets.partitionsToOffsetAndMetadata()));
    CompletableFuture<ListOffsetsOptions> listOffsetsOptions =
        CompletableFuture.supplyAsync(
            () -> new ListOffsetsOptions(isolationLevel));
    CompletableFuture<Map<TopicPartition, OffsetSpec>> latestOffsetSpecs = fetchedCurrentOffsets
        .thenApply(map ->
            map.keySet().stream()
                .collect(Collectors.toMap(Function.identity(), tp -> OffsetSpec.latest())));
  }

//  @Override
//  public CompletableFuture<Optional<ConsumerLag>> getConsumerLag(
//      String clusterId, String topicName, Integer partitionId, String consumerGroupId) {
//    return listConsumerLags(clusterId, consumerGroupId)
//        .thenApply(
//            lags ->
//                lags.stream()
//                    .filter(lag -> lag.getTopicName().equals(topicName))
//                    .filter(lag -> lag.getPartitionId() == partitionId)
//                    .filter(lag -> lag.getConsumerGroupId().equals(consumerGroupId))
//                    .findAny());
//  }
}
