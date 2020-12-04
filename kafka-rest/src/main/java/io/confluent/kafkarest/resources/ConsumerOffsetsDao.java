package io.confluent.kafkarest.resources;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Collectors;
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

public class ConsumerOffsetsDao {

  private final String clusterId;
  private final Admin kafkaAdminClient;
  private final int consumerMetadataTimeout;

  public ConsumerOffsetsDao(
      String clusterId,
      Admin kafkaAdminClient,
      int consumerMetadataTimeout
  ) {
    this.clusterId = clusterId;
    this.kafkaAdminClient = kafkaAdminClient;
    this.consumerMetadataTimeout = consumerMetadataTimeout;
  }

  public Map<String, ConsumerGroupOffsets> getAllConsumerGroupOffsets()
      throws InterruptedException, ExecutionException, TimeoutException {

    final Set<String> consumerGroupIds = getConsumerGroups();
    final Map<String, ConsumerGroupDescription> fetchedConsumerGroupDesc =
        getAllConsumerGroupDescriptions(consumerGroupIds);
    final Map<String, ConsumerGroupOffsets> cgOffsetsMap = new HashMap<>();

    // all consumer groups for this cluster
    for (ConsumerGroupDescription desc : fetchedConsumerGroupDesc.values()) {
      ConsumerGroupOffsets cgOffsets = getConsumerGroupOffsets(desc, IsolationLevel.READ_COMMITTED);
      cgOffsetsMap.put(desc.groupId(), cgOffsets);
    }

    return cgOffsetsMap;
  }

  ConsumerGroupOffsets getConsumerGroupOffsets(
      ConsumerGroupDescription cgDesc,
      Map<TopicPartition, OffsetAndMetadata> fetchedCurrentOffsets,
      Map<TopicPartition, ListOffsetsResultInfo> earliestOffsets,
      Map<TopicPartition, ListOffsetsResultInfo> latestOffsets
  ) {
    ConsumerGroupOffsets cgOffsets = new ConsumerGroupOffsets(cgDesc.groupId());

    // build map of topic partition -> consumer id
    Map<TopicPartition, String> tpConsumerIds = new HashMap<>();
    Map<TopicPartition, String> tpClientIds = new HashMap<>();

    for (MemberDescription memberDesc : cgDesc.members()) {
      for (TopicPartition tp : memberDesc.assignment().topicPartitions()) {
        tpConsumerIds.put(tp, memberDesc.consumerId());
        tpClientIds.put(tp, memberDesc.clientId());
      }
    }

    for (TopicPartition tp : fetchedCurrentOffsets.keySet()) {
      // look up consumer id from map if not part of a simple consumer group
      String consumerId = tpConsumerIds.getOrDefault(tp, "");
      String clientId = tpClientIds.getOrDefault(tp, "");

      long currentOffset = getCurrentOffset(fetchedCurrentOffsets, tp);
      long latestOffset = getOffset(latestOffsets, tp);
      long earliestOffset = getOffset(earliestOffsets, tp);
      if (currentOffset < 0 || latestOffset < 0) {
//        log.debug("invalid offsets for topic={} consumerId={} current={} latest={}",
//            tp.topic(),
//            consumerId,
//            currentOffset,
//            latestOffset);
        continue;
      }
      cgOffsets.addOffset(
          tp.topic(),
          consumerId,
          clientId,
          tp.partition(),
          currentOffset,
          earliestOffset,
          latestOffset
      );
    }
    return cgOffsets;
  }

  public ConsumerGroupOffsets getConsumerGroupOffsets(
      String consumerGroupId,
      IsolationLevel isolationLevel
  ) throws InterruptedException, ExecutionException, TimeoutException {
    final ConsumerGroupDescription cgDesc = getConsumerGroupDescription(consumerGroupId);
    return getConsumerGroupOffsets(cgDesc, isolationLevel);
  }

  private ConsumerGroupOffsets getConsumerGroupOffsets(
      ConsumerGroupDescription desc,
      IsolationLevel isolationLevel
  ) throws InterruptedException, ExecutionException, TimeoutException {
    final Map<TopicPartition, OffsetAndMetadata> fetchedCurrentOffsets =
        getCurrentOffsets(desc.groupId());

    ListOffsetsOptions listOffsetsOptions = new ListOffsetsOptions(isolationLevel)
        .timeoutMs(consumerMetadataTimeout);

    Map<TopicPartition, OffsetSpec> latestOffsetSpecs = fetchedCurrentOffsets.keySet().stream()
        .collect(Collectors.toMap(Function.identity(), tp -> OffsetSpec.latest()));
    Map<TopicPartition, OffsetSpec> earliestOffsetSpecs = fetchedCurrentOffsets.keySet().stream()
        .collect(Collectors.toMap(Function.identity(), tp -> OffsetSpec.earliest()));

    ListOffsetsResult latestOffsetResult = kafkaAdminClient.listOffsets(
        latestOffsetSpecs, listOffsetsOptions);
    ListOffsetsResult earliestOffsetResult = kafkaAdminClient.listOffsets(
        earliestOffsetSpecs, listOffsetsOptions);
    Map<TopicPartition, ListOffsetsResultInfo> latestOffsets = latestOffsetResult.all()
        .get(consumerMetadataTimeout, TimeUnit.MILLISECONDS);
    Map<TopicPartition, ListOffsetsResultInfo> earliestOffsets = earliestOffsetResult.all()
        .get(consumerMetadataTimeout, TimeUnit.MILLISECONDS);

    return getConsumerGroupOffsets(
        desc,
        fetchedCurrentOffsets,
        earliestOffsets,
        latestOffsets
    );
  }

  public String clusterId() {
    return clusterId;
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
//                      log.warn("failed fetching description for consumerGroup={}", cgId,
//                          throwable);
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

//  @Override
//  public void close() {
//    kafkaAdminClient.close();
//  }
}
