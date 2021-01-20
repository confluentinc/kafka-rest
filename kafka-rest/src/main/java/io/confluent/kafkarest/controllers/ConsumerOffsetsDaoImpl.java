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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.inject.Inject;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsOptions;
import org.apache.kafka.clients.admin.ListOffsetsOptions;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.ConsumerGroupState;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.TopicPartition;

final class ConsumerOffsetsDaoImpl implements ConsumerOffsetsDao {

  private final Admin kafkaAdminClient;
  @Inject
  ConsumerOffsetsDaoImpl(
      Admin kafkaAdminClient
  ) {
    this.kafkaAdminClient = kafkaAdminClient;
  }

  @AutoValue
  abstract static class MemberId {

    abstract String getConsumerId();

    abstract String getClientId();

    abstract Optional<String> getInstanceId();

    public static Builder builder() {
      return new AutoValue_ConsumerOffsetsDaoImpl_MemberId.Builder();
    }

    @AutoValue.Builder
    abstract static class Builder {

      abstract Builder setConsumerId(String consumerId);

      abstract Builder setClientId(String clientId);

      abstract Builder setInstanceId(Optional<String> instanceId);

      abstract MemberId build();
    }
  }

  @Override
  public CompletableFuture<Map<TopicPartition, MemberId>> getMemberIds(
      ConsumerGroupDescription cgDesc
  ) {
    Map<TopicPartition, MemberId> tpMemberIds = new HashMap<>();
    for (MemberDescription memberDesc : cgDesc.members()) {
      for (TopicPartition tp : memberDesc.assignment().topicPartitions()) {
        MemberId memberId =
            MemberId.builder()
                .setConsumerId(memberDesc.consumerId())
                .setClientId(memberDesc.clientId())
                .setInstanceId(memberDesc.groupInstanceId())
                .build();
        tpMemberIds.put(tp, memberId);
      }
    }
    return CompletableFuture.completedFuture(tpMemberIds);
  }

  @Override
  public CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> getCurrentOffsets(
      String consumerGroupId
  ) {
    return KafkaFutures.toCompletableFuture(
        kafkaAdminClient.listConsumerGroupOffsets(
            consumerGroupId, new ListConsumerGroupOffsetsOptions())
            .partitionsToOffsetAndMetadata());
  }

  @Override
  public CompletableFuture<Map<TopicPartition, ListOffsetsResultInfo>> getLatestOffsets(
      IsolationLevel isolationLevel,
      Map<TopicPartition, OffsetAndMetadata> currentOffsets
  ) {
    Map<TopicPartition, OffsetSpec> latestOffsetSpecs =
        currentOffsets.keySet()
            .stream()
            .collect(
                Collectors.toMap(Function.identity(), tp -> OffsetSpec.latest()));
    return KafkaFutures.toCompletableFuture(
        kafkaAdminClient.listOffsets(
            latestOffsetSpecs,
            new ListOffsetsOptions(isolationLevel)).all());
  }

  @Override
  public CompletableFuture<Optional<ConsumerGroupDescription>> getConsumerGroupDescription(
      String consumerGroupId) {
    return KafkaFutures.toCompletableFuture(
        kafkaAdminClient.describeConsumerGroups(Collections.singletonList(consumerGroupId)).all())
        .thenApply(
            descriptions ->
                descriptions.values()
                    .stream()
                    .filter(
                        // When describing a consumer-group that does not exist, AdminClient returns
                        // a dummy consumer-group with simple=true and state=DEAD.
                        // TODO: Investigate a better way of detecting non-existent consumer-group.
                        description -> !description.isSimpleConsumerGroup()
                            || description.state() != ConsumerGroupState.DEAD)
                    .findAny());
  }


  @Override
  public long getCurrentOffset(Map<TopicPartition, OffsetAndMetadata> map, TopicPartition tp) {
    if (map == null) {
      return -1;
    }
    OffsetAndMetadata oam = map.get(tp);
    if (oam == null) {
      return -1;
    }
    return oam.offset();
  }

  @Override
  public long getOffset(Map<TopicPartition, ListOffsetsResultInfo> map, TopicPartition tp) {
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
