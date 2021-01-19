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
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import io.confluent.kafkarest.common.KafkaFutures;
import io.confluent.kafkarest.config.ConfigModule.OffsetsTimeoutConfig;
import io.confluent.kafkarest.entities.ConsumerGroupLag;
import io.confluent.kafkarest.entities.ConsumerLag;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.ws.rs.NotFoundException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.DescribeConsumerGroupsOptions;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsOptions;
import org.apache.kafka.clients.admin.ListConsumerGroupsOptions;
import org.apache.kafka.clients.admin.ListOffsetsOptions;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.ConsumerGroupState;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;

final class ConsumerOffsetsDaoImpl implements ConsumerOffsetsDao {

  private final Admin kafkaAdminClient;
  private final int consumerMetadataTimeout;

  @Inject
  ConsumerOffsetsDaoImpl(
      Admin kafkaAdminClient,
      @OffsetsTimeoutConfig Duration consumerMetadataTimeout
  ) {
    this.kafkaAdminClient = kafkaAdminClient;
    // ahu todo: figure out best way to safely cast to int (required by AbstractOptions.timeoutMs())
    // this.consumerMetadataTimeout = Math.toIntExact(Long.MAX_VALUE);
    this.consumerMetadataTimeout = Math.toIntExact(
        Math.min(consumerMetadataTimeout.toMillis(), Integer.MAX_VALUE));
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
  public Admin getKafkaAdminClient() {
    return kafkaAdminClient;
  }

  public List<ConsumerLag> getConsumerLags(
      String clusterId,
      String consumerGroupId,
      IsolationLevel isolationLevel
  ) throws InterruptedException, ExecutionException, TimeoutException {
    List<ConsumerLag> consumerLags = new ArrayList<>();
    final ConsumerGroupDescription cgDesc = getConsumerGroupDescription(consumerGroupId);

    final Map<TopicPartition, OffsetAndMetadata> fetchedCurrentOffsets =
        getCurrentOffsets(consumerGroupId);

    ListOffsetsOptions listOffsetsOptions = new ListOffsetsOptions(isolationLevel)
        .timeoutMs(consumerMetadataTimeout);

    Map<TopicPartition, OffsetSpec> latestOffsetSpecs = fetchedCurrentOffsets.keySet().stream()
        .collect(Collectors.toMap(Function.identity(), tp -> OffsetSpec.latest()));

    ListOffsetsResult latestOffsetResult = kafkaAdminClient.listOffsets(
        latestOffsetSpecs, listOffsetsOptions);
    // wrap this in a future
    // make sure there are no gets in a future, will block until future is complete
    // look into kafka limits
    // follow up on thread w/ ismael
    Map<TopicPartition, ListOffsetsResultInfo> latestOffsets = latestOffsetResult.all()
        .get(consumerMetadataTimeout, TimeUnit.MILLISECONDS);

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

    for (TopicPartition tp : fetchedCurrentOffsets.keySet()) {
      // look up consumer id from map if not part of a simple consumer group
      MemberId memberId = tpMemberIds.getOrDefault(
          tp, MemberId.builder()
              .setConsumerId("")
              .setClientId("")
              .setInstanceId(Optional.empty())
              .build());

      long currentOffset = getCurrentOffset(fetchedCurrentOffsets, tp);
      long latestOffset = getOffset(latestOffsets, tp);
      if (currentOffset < 0 || latestOffset < 0) {
        // log.debug("invalid offsets for topic={} consumerId={} current={} latest={}",
        //     tp.topic(),
        //     consumerId,
        //     currentOffset,
        //     latestOffset);
        continue;
      }
      ConsumerLag consumerLag =
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
              .build();
      consumerLags.add(consumerLag);
    }
    return consumerLags;
  }

  ConsumerGroupLag.Builder getConsumerGroupOffsets(
      ConsumerGroupDescription cgDesc,
      Map<TopicPartition, OffsetAndMetadata> fetchedCurrentOffsets,
      Map<TopicPartition, ListOffsetsResultInfo> latestOffsets
  ) {
    ConsumerGroupLag.Builder cgOffsets =
        ConsumerGroupLag.builder().setConsumerGroupId(cgDesc.groupId());

    // build map of topic partition -> consumer, client, instance ids
    // Map<TopicPartition, String> tpConsumerIds = new HashMap<>();
    // Map<TopicPartition, String> tpClientIds = new HashMap<>();
    // Map<TopicPartition, Optional<String>> tpInstanceIds = new HashMap<>();
    Map<TopicPartition, MemberId> tpMemberIds = new HashMap<>();

    for (MemberDescription memberDesc : cgDesc.members()) {
      for (TopicPartition tp : memberDesc.assignment().topicPartitions()) {
        // tpConsumerIds.put(tp, memberDesc.consumerId());
        // tpClientIds.put(tp, memberDesc.clientId());
        // tpInstanceIds.put(tp, memberDesc.groupInstanceId());
        MemberId memberId =
            MemberId.builder()
                .setConsumerId(memberDesc.consumerId())
                .setClientId(memberDesc.clientId())
                .setInstanceId(memberDesc.groupInstanceId())
                .build();
        tpMemberIds.put(tp, memberId);
      }
    }

    for (TopicPartition tp : fetchedCurrentOffsets.keySet()) {
      // look up consumer id from map if not part of a simple consumer group
      // String consumerId = tpConsumerIds.getOrDefault(tp, "");
      // String clientId = tpClientIds.getOrDefault(tp, "");
      // Optional<String> instanceId = tpInstanceIds.getOrDefault(tp, Optional.empty());
      MemberId memberId = tpMemberIds.getOrDefault(
          tp, MemberId.builder()
              .setConsumerId("")
              .setClientId("")
              .setInstanceId(Optional.empty())
              .build());

      long currentOffset = getCurrentOffset(fetchedCurrentOffsets, tp);
      long latestOffset = getOffset(latestOffsets, tp);
      if (currentOffset < 0 || latestOffset < 0) {
        // log.debug("invalid offsets for topic={} consumerId={} current={} latest={}",
        //     tp.topic(),
        //     consumerId,
        //     currentOffset,
        //     latestOffset);
        continue;
      }
      cgOffsets.addOffset(
          tp.topic(),
          memberId.getConsumerId(),
          memberId.getClientId(),
          memberId.getInstanceId(),
          tp.partition(),
          currentOffset,
          latestOffset
      );
    }
    return cgOffsets;
  }

  @Override
  public CompletableFuture<Map<TopicPartition, MemberId>> getMemberIds(
      CompletableFuture<ConsumerGroupDescription> cgDesc
  ) {
    CompletableFuture<Map<TopicPartition, MemberId>> tpMemberIds =
        CompletableFuture.supplyAsync(HashMap::new);
    cgDesc.thenCombine(
        tpMemberIds, (desc, memberIds) -> {
          desc.members().stream()
              .forEach(
                  memberDesc ->
                      memberDesc.assignment().topicPartitions()
                          .stream()
                          .forEach(
                              tp ->
                                  memberIds.put(
                                      tp, MemberId.builder()
                                          .setConsumerId(memberDesc.consumerId())
                                          .setClientId(memberDesc.clientId())
                                          .setInstanceId(memberDesc.groupInstanceId())
                                          .build())));
          return null; });
    return tpMemberIds;
  }

  public ConsumerGroupLag getConsumerGroupOffsets(
      String clusterId,
      String consumerGroupId,
      IsolationLevel isolationLevel
  ) throws InterruptedException, ExecutionException, TimeoutException {
    final ConsumerGroupDescription cgDesc = getConsumerGroupDescription(consumerGroupId);
    return getConsumerGroupOffsets(cgDesc, isolationLevel).setClusterId(clusterId).build();
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
      CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> currentOffsets
  ) {
    return currentOffsets
        .thenApply(
            map ->
                map.keySet().stream()
                    .collect(Collectors.toMap(Function.identity(), tp -> OffsetSpec.latest())))
        .thenApply(
            specs ->
                kafkaAdminClient.listOffsets(specs, new ListOffsetsOptions(isolationLevel)))
        .thenCompose(result ->
            KafkaFutures.toCompletableFuture(result.all()));
  }

  private ConsumerGroupLag.Builder getConsumerGroupOffsets(
      ConsumerGroupDescription desc,
      IsolationLevel isolationLevel
  ) throws InterruptedException, ExecutionException, TimeoutException {
    final Map<TopicPartition, OffsetAndMetadata> fetchedCurrentOffsets =
        getCurrentOffsets(desc.groupId());

    ListOffsetsOptions listOffsetsOptions = new ListOffsetsOptions(isolationLevel)
        .timeoutMs(consumerMetadataTimeout);

    Map<TopicPartition, OffsetSpec> latestOffsetSpecs = fetchedCurrentOffsets.keySet().stream()
        .collect(Collectors.toMap(Function.identity(), tp -> OffsetSpec.latest()));

    ListOffsetsResult latestOffsetResult = kafkaAdminClient.listOffsets(
        latestOffsetSpecs, listOffsetsOptions);
    Map<TopicPartition, ListOffsetsResultInfo> latestOffsets = latestOffsetResult.all()
        .get(consumerMetadataTimeout, TimeUnit.MILLISECONDS);

    return getConsumerGroupOffsets(
        desc,
        fetchedCurrentOffsets,
        latestOffsets
    );
  }

//  public Set<String> getConsumerGroups()
//      throws InterruptedException, ExecutionException, TimeoutException {
//    return Sets.newLinkedHashSet(
//        Iterables.transform(kafkaAdminClient
//                .listConsumerGroups(new ListConsumerGroupsOptions()
//                    .timeoutMs(consumerMetadataTimeout))
//                .all()
//                .get(consumerMetadataTimeout, TimeUnit.MILLISECONDS),
//            ConsumerGroupListing::groupId));
//  }

  // have consumeroffsetsdaoimpl just have the shared functions, everythign else in lagmanager
  // shared by get and list
//  public ConsumerGroupDescription getConsumerGroupDescription(
//      String consumerGroupId
//  ) throws InterruptedException, ExecutionException, TimeoutException {
//    Map<String, ConsumerGroupDescription> allCgDesc =
//        getAllConsumerGroupDescriptions(ImmutableSet.of(consumerGroupId));
//    return allCgDesc.get(consumerGroupId);
//  }

  @Override
  public CompletableFuture<ConsumerGroupDescription> getConsumerGroupDescription(
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
                    .findAny()
                    .orElseThrow(
                        () -> new NotFoundException("Consumer group " +
                            consumerGroupId + " could not be found.")));  // not useful currently
  }

  public Map<String, ConsumerGroupDescription> getAllConsumerGroupDescriptions(
      Collection<String> consumerGroupIds
  ) throws InterruptedException, ExecutionException, TimeoutException {

    final Map<String, ConsumerGroupDescription> ret = new HashMap<>();

    KafkaFuture.allOf(Iterables.toArray(
        Iterables.transform(
            kafkaAdminClient
                .describeConsumerGroups(
                    consumerGroupIds,
                    new DescribeConsumerGroupsOptions()
                        .includeAuthorizedOperations(true)
                        .timeoutMs(consumerMetadataTimeout)
                )
                .describedGroups().entrySet(),
            entry -> {
              final String cgId = entry.getKey();
              return entry.getValue().whenComplete(
                  (cgDesc, throwable) -> {
                    if (throwable != null) {
                      // log.warn("failed fetching description for consumerGroup={}", cgId,
                      //     throwable);
                    } else if (cgDesc != null) {
                      ret.put(cgId, cgDesc);
                    }
                  }
              );
            }
        ), KafkaFuture.class)
    ).get(consumerMetadataTimeout, TimeUnit.MILLISECONDS);

    return ret;
  }

//  public Map<TopicPartition, OffsetAndMetadata> getCurrentOffsets(
//      String consumerGroupId
//  ) throws InterruptedException, ExecutionException, TimeoutException {
//    return kafkaAdminClient
//        .listConsumerGroupOffsets(consumerGroupId,
//            new ListConsumerGroupOffsetsOptions().timeoutMs(consumerMetadataTimeout))
//        .partitionsToOffsetAndMetadata().get(consumerMetadataTimeout, TimeUnit.MILLISECONDS);
//  }

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
