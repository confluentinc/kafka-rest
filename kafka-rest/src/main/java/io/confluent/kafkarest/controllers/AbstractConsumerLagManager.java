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

import com.google.auto.value.AutoValue;
import io.confluent.kafkarest.common.KafkaFutures;
import io.confluent.kafkarest.entities.Consumer;
import io.confluent.kafkarest.entities.ConsumerGroup;
import io.confluent.kafkarest.entities.Partition;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.ws.rs.NotFoundException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsOptions;
import org.apache.kafka.clients.admin.ListOffsetsOptions;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.TopicPartition;

abstract class AbstractConsumerLagManager {

  private final Admin kafkaAdminClient;
  private final static IsolationLevel ISOLATION_LEVEL = IsolationLevel.READ_COMMITTED;

  @Inject
  AbstractConsumerLagManager(
      Admin kafkaAdminClient
  ) {
    this.kafkaAdminClient = kafkaAdminClient;
  }

  protected final CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> getCurrentOffsets(
      String consumerGroupId
  ) {
    return KafkaFutures.toCompletableFuture(
        kafkaAdminClient.listConsumerGroupOffsets(
            consumerGroupId, new ListConsumerGroupOffsetsOptions())
            .partitionsToOffsetAndMetadata());
  }

  protected final CompletableFuture<Map<TopicPartition, ListOffsetsResultInfo>> getLatestOffsets(
      Map<TopicPartition, OffsetAndMetadata> currentOffsets
  ) {
    Map<TopicPartition, OffsetSpec> latestOffsetSpecs =
        currentOffsets.keySet()
            .stream()
            .collect(
                Collectors.toMap(Function.identity(), topicPartition -> OffsetSpec.latest()));
    return KafkaFutures.toCompletableFuture(
        kafkaAdminClient.listOffsets(
            latestOffsetSpecs,
            new ListOffsetsOptions(ISOLATION_LEVEL)).all());
  }

  protected static final Map<TopicPartition, Consumer> getPartitionAssignment(
      ConsumerGroup consumerGroup
  ) {
    Map<TopicPartition, Consumer> partitionAssignment = new HashMap<>();
    for (Consumer consumer : consumerGroup.getConsumers()) {
      for (Partition partition : consumer.getAssignedPartitions()) {
        partitionAssignment.put(
            new TopicPartition(partition.getTopicName(), partition.getPartitionId()),
            consumer);
      }
    }
    return partitionAssignment;
  }

//  protected static final Map<TopicPartition, MemberId> getMemberIds(
//      ConsumerGroup consumerGroup
//  ) {
//    Map<TopicPartition, MemberId> tpMemberIds = new HashMap<>();
//    for (Consumer consumer: consumerGroup.getConsumers()) {
//      for (Partition partition : consumer.getAssignedPartitions()) {
//        MemberId memberId =
//            MemberId.builder()
//                .setConsumerId(consumer.getConsumerId())
//                .setInstanceId(consumer.getInstanceId().orElse(null))
//                .setClientId(consumer.getClientId())
//                .build();
//        TopicPartition topicPartition =
//            new TopicPartition(partition.getTopicName(), partition.getPartitionId());
//        tpMemberIds.put(topicPartition, memberId);
//      }
//    }
//    return tpMemberIds;
//  }

  protected static final Optional<Long> getCurrentOffset(
      Map<TopicPartition, OffsetAndMetadata> currentOffsets,
      TopicPartition topicPartition
  ) {
    OffsetAndMetadata offsetAndMetadata = currentOffsets.get(topicPartition);
    if (offsetAndMetadata == null) {
      return Optional.empty();
    }
    return Optional.of(offsetAndMetadata.offset());
  }

  protected static final Optional<Long> getLatestOffset(
      Map<TopicPartition, ListOffsetsResultInfo> latestOffsets,
      TopicPartition topicPartition
  ) {
    ListOffsetsResultInfo offsetInfo = latestOffsets.get(topicPartition);
    if (offsetInfo == null) {
      return Optional.empty();
    }
    return Optional.of(offsetInfo.offset());
  }

  protected static final <T extends Map<?, ?>> T checkOffsetsExist(
      T offsets,
      String message,
      Object... args
  ) {
    if (offsets.isEmpty()) {
      throw new NotFoundException(String.format(message, args));
    }
    return offsets;
  }

//  @AutoValue
//  protected abstract static class MemberId {
//
//    protected MemberId() {
//    }
//
//    protected abstract String getConsumerId();
//
//    protected abstract Optional<String> getInstanceId();
//
//    protected abstract String getClientId();
//
//    protected static Builder builder() {
//      return new AutoValue_AbstractConsumerLagManager_MemberId.Builder();
//    }
//
//    @AutoValue.Builder
//    protected abstract static class Builder {
//
//      protected Builder() {
//      }
//
//      protected abstract Builder setConsumerId(String consumerId);
//
//      protected abstract Builder setInstanceId(@Nullable String instanceId);
//
//      protected abstract Builder setClientId(String clientId);
//
//      protected abstract MemberId build();
//    }
//  }
}
