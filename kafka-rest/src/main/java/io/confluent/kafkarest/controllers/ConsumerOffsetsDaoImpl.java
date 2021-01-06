package io.confluent.kafkarest.controllers;

import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import io.confluent.kafkarest.config.ConfigModule.OffsetsTimeoutConfig;
import io.confluent.kafkarest.entities.ConsumerGroupLag;
import io.confluent.kafkarest.entities.ConsumerLag;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.inject.Inject;
import okio.Timeout;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Array;
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

  // public Map<String, ConsumerGroupOffsets> getAllConsumerGroupOffsets()
  //     throws InterruptedException, ExecutionException, TimeoutException {

  //   final Set<String> consumerGroupIds = getConsumerGroups();
  //   final Map<String, ConsumerGroupDescription> fetchedConsumerGroupDesc =
  //       getAllConsumerGroupDescriptions(consumerGroupIds);
  //   final Map<String, ConsumerGroupOffsets> cgOffsetsMap = new HashMap<>();

  //   // all consumer groups for this cluster
  //   for (ConsumerGroupDescription desc : fetchedConsumerGroupDesc.values()) {
  //     ConsumerGroupOffsets cgOffsets = getConsumerGroupOffsets(desc, IsolationLevel.READ_COMMITTED);
  //     cgOffsetsMap.put(desc.groupId(), cgOffsets);
  //   }

  //   return cgOffsetsMap;
  // }

  @AutoValue
  abstract static class MemberId {

    abstract String getConsumerId();

    abstract String getClientId();

    abstract Optional<String> getInstanceId();

    private static Builder builder() { return new AutoValue_ConsumerOffsetsDaoImpl_MemberId.Builder();}

    @AutoValue.Builder
    abstract static class Builder {

      abstract Builder setConsumerId(String consumerId);

      abstract Builder setClientId(String clientId);

      abstract Builder setInstanceId(Optional<String> instanceId);

      abstract MemberId build();
    }
  }

  public List<ConsumerLag> getConsumerLags(
      String clusterId,
      String consumerGroupId,
      IsolationLevel isolationLevel
  ) throws InterruptedException, ExecutionException, TimeoutException {
    List<ConsumerLag> consumerLags = new ArrayList<>();
    final ConsumerGroupDescription cgDesc = getConsumerGroupDescription(consumerGroupId);

    final Map<TopicPartition, OffsetAndMetadata> fetchedCurrentOffsets =
        getCurrentOffsets(cgDesc.groupId());

    ListOffsetsOptions listOffsetsOptions = new ListOffsetsOptions(isolationLevel)
        .timeoutMs(consumerMetadataTimeout);

    Map<TopicPartition, OffsetSpec> latestOffsetSpecs = fetchedCurrentOffsets.keySet().stream()
        .collect(Collectors.toMap(Function.identity(), tp -> OffsetSpec.latest()));

    ListOffsetsResult latestOffsetResult = kafkaAdminClient.listOffsets(
        latestOffsetSpecs, listOffsetsOptions);
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

  public ConsumerGroupLag getConsumerGroupOffsets(
      String clusterId,
      String consumerGroupId,
      IsolationLevel isolationLevel
  ) throws InterruptedException, ExecutionException, TimeoutException {
    final ConsumerGroupDescription cgDesc = getConsumerGroupDescription(consumerGroupId);
    return getConsumerGroupOffsets(cgDesc, isolationLevel).setClusterId(clusterId).build();
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

  public Set<String> getConsumerGroups()
      throws InterruptedException, ExecutionException, TimeoutException {
    return Sets.newLinkedHashSet(
        Iterables.transform(kafkaAdminClient
                .listConsumerGroups(new ListConsumerGroupsOptions()
                    .timeoutMs(consumerMetadataTimeout))
                .all()
                .get(consumerMetadataTimeout, TimeUnit.MILLISECONDS),
            ConsumerGroupListing::groupId));
  }

  public ConsumerGroupDescription getConsumerGroupDescription(
      String consumerGroupId
  ) throws InterruptedException, ExecutionException, TimeoutException {
    Map<String, ConsumerGroupDescription> allCgDesc =
        getAllConsumerGroupDescriptions(ImmutableSet.of(consumerGroupId));
    return allCgDesc.get(consumerGroupId);
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

  public Map<TopicPartition, OffsetAndMetadata> getCurrentOffsets(
      String consumerGroupId
  ) throws InterruptedException, ExecutionException, TimeoutException {
    return kafkaAdminClient
        .listConsumerGroupOffsets(consumerGroupId,
            new ListConsumerGroupOffsetsOptions().timeoutMs(consumerMetadataTimeout))
        .partitionsToOffsetAndMetadata().get(consumerMetadataTimeout, TimeUnit.MILLISECONDS);
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
