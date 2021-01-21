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

  @Inject
  AbstractConsumerLagManager(
      Admin kafkaAdminClient
  ) {
    this.kafkaAdminClient = kafkaAdminClient;
  }

  protected Map<TopicPartition, MemberId> getMemberIds(
      ConsumerGroup consumerGroup
  ) {
    Map<TopicPartition, MemberId> tpMemberIds = new HashMap<>();
    for (Consumer consumer: consumerGroup.getConsumers()) {
      for (Partition partition : consumer.getAssignedPartitions()) {
        MemberId memberId =
            MemberId.builder()
                .setConsumerId(consumer.getConsumerId())
                .setClientId(consumer.getClientId())
                .setInstanceId(consumer.getInstanceId().orElse(null))
                .build();
        TopicPartition topicPartition =
            new TopicPartition(partition.getTopicName(), partition.getPartitionId());
        tpMemberIds.put(topicPartition, memberId);
      }
    }
    return tpMemberIds;
  }

  protected CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> getCurrentOffsets(
      String consumerGroupId
  ) {
    return KafkaFutures.toCompletableFuture(
        kafkaAdminClient.listConsumerGroupOffsets(
            consumerGroupId, new ListConsumerGroupOffsetsOptions())
            .partitionsToOffsetAndMetadata());
  }

  protected CompletableFuture<Map<TopicPartition, ListOffsetsResultInfo>> getLatestOffsets(
      Map<TopicPartition, OffsetAndMetadata> currentOffsets
  ) {
    IsolationLevel isolationLevel = IsolationLevel.READ_COMMITTED;
    Map<TopicPartition, OffsetSpec> latestOffsetSpecs =
        currentOffsets.keySet()
            .stream()
            .collect(
                Collectors.toMap(Function.identity(), topicPartition -> OffsetSpec.latest()));
    return KafkaFutures.toCompletableFuture(
        kafkaAdminClient.listOffsets(
            latestOffsetSpecs,
            new ListOffsetsOptions(isolationLevel)).all());
  }

  protected long getCurrentOffset(Map<TopicPartition, OffsetAndMetadata> map, TopicPartition topicPartition) {
    if (map == null) {
      return -1;
    }
    OffsetAndMetadata oam = map.get(topicPartition);
    if (oam == null) {
      return -1;
    }
    return oam.offset();
  }

  protected long getOffset(Map<TopicPartition, ListOffsetsResultInfo> map, TopicPartition topicPartition) {
    if (map == null) {
      return -1;
    }

    ListOffsetsResultInfo offsetInfo = map.get(topicPartition);
    if (offsetInfo == null) {
      return -1;
    }

    return offsetInfo.offset();
  }

  @AutoValue
  abstract static class MemberId {

    protected MemberId() {
    }

    abstract String getConsumerId();

    abstract String getClientId();

    abstract Optional<String> getInstanceId();

    protected static Builder builder() {
      return new AutoValue_AbstractConsumerLagManager_MemberId.Builder();
    }

    @AutoValue.Builder
    abstract static class Builder {

      protected Builder() {
      }

      abstract Builder setConsumerId(String consumerId);

      abstract Builder setClientId(String clientId);

      abstract Builder setInstanceId(@Nullable String instanceId);

      abstract MemberId build();
    }
  }
}
