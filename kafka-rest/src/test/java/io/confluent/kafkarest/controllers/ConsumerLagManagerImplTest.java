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

import static java.util.Collections.emptyList;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.newCapture;
import static org.easymock.EasyMock.replay;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import io.confluent.kafkarest.entities.Broker;
import io.confluent.kafkarest.entities.Consumer;
import io.confluent.kafkarest.entities.ConsumerGroup;
import io.confluent.kafkarest.entities.ConsumerGroup.State;
import io.confluent.kafkarest.entities.ConsumerLag;
import io.confluent.kafkarest.entities.Partition;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import javax.ws.rs.NotFoundException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsOptions;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.ListOffsetsOptions;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.easymock.Capture;
import org.easymock.EasyMockExtension;
import org.easymock.Mock;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(EasyMockExtension.class)
public class ConsumerLagManagerImplTest {

  private static final String CLUSTER_ID = "cluster-1";

  private static final Broker BROKER_1 =
      Broker.create(
          CLUSTER_ID, /* brokerId= */ 1, /* host= */ "1.2.3.4", /* port= */ 1000, /* rack= */ null);

  private static final String CONSUMER_GROUP_ID = "consumer-group-1";

  private static final Consumer CONSUMER_1 =
      Consumer.builder()
          .setClusterId(CLUSTER_ID)
          .setConsumerGroupId(CONSUMER_GROUP_ID)
          .setConsumerId("consumer-1")
          .setInstanceId("instance-1")
          .setClientId("client-1")
          .setHost("11.12.12.14")
          .setAssignedPartitions(
              Arrays.asList(
                  Partition.create(
                      CLUSTER_ID,
                      /* topicName= */ "topic-1",
                      /* partitionId= */ 1,
                      /* replicas= */ emptyList()),
                  Partition.create(
                      CLUSTER_ID,
                      /* topicName= */ "topic-3",
                      /* partitionId= */ 3,
                      /* replicas= */ emptyList())))
          .build();

  private static final Consumer CONSUMER_2 =
      Consumer.builder()
          .setClusterId(CLUSTER_ID)
          .setConsumerGroupId(CONSUMER_GROUP_ID)
          .setConsumerId("consumer-2")
          .setInstanceId("instance-2")
          .setClientId("client-2")
          .setHost("11.12.12.14")
          .setAssignedPartitions(
              Collections.singletonList(
                  Partition.create(
                      CLUSTER_ID,
                      /* topicName= */ "topic-2",
                      /* partitionId= */ 2,
                      /* replicas= */ emptyList())))
          .build();

  private static final ConsumerGroup CONSUMER_GROUP =
      ConsumerGroup.builder()
          .setClusterId(CLUSTER_ID)
          .setConsumerGroupId(CONSUMER_GROUP_ID)
          .setSimple(true)
          .setPartitionAssignor("org.apache.kafka.clients.consumer.RangeAssignor")
          .setState(State.STABLE)
          .setCoordinator(BROKER_1)
          .setConsumers(Arrays.asList(CONSUMER_1, CONSUMER_2))
          .build();

  private static final TopicPartition TOPIC_PARTITION_1 = new TopicPartition("topic-1", 1);

  private static final TopicPartition TOPIC_PARTITION_2 = new TopicPartition("topic-2", 2);

  private static final TopicPartition TOPIC_PARTITION_3 = new TopicPartition("topic-3", 3);

  private static final Map<TopicPartition, OffsetAndMetadata> OFFSET_AND_METADATA_MAP;

  static {
    OFFSET_AND_METADATA_MAP = new HashMap<>();
    OFFSET_AND_METADATA_MAP.put(TOPIC_PARTITION_1, new OffsetAndMetadata(0));
    OFFSET_AND_METADATA_MAP.put(TOPIC_PARTITION_2, new OffsetAndMetadata(100));
    OFFSET_AND_METADATA_MAP.put(TOPIC_PARTITION_3, new OffsetAndMetadata(110));
  }

  private static final Map<TopicPartition, KafkaFuture<ListOffsetsResultInfo>> LATEST_OFFSETS_MAP;

  static {
    LATEST_OFFSETS_MAP = new HashMap<>();
    LATEST_OFFSETS_MAP.put(
        TOPIC_PARTITION_1, KafkaFuture.completedFuture(new ListOffsetsResultInfo(100L, 0L, null)));
    LATEST_OFFSETS_MAP.put(
        TOPIC_PARTITION_2, KafkaFuture.completedFuture(new ListOffsetsResultInfo(100L, 0L, null)));
    LATEST_OFFSETS_MAP.put(
        TOPIC_PARTITION_3, KafkaFuture.completedFuture(new ListOffsetsResultInfo(100L, 0L, null)));
  }

  private static final Map<TopicPartition, ListOffsetsResultInfo> LATEST_OFFSETS_MAP_2;

  static {
    LATEST_OFFSETS_MAP_2 = new HashMap<>();
    LATEST_OFFSETS_MAP_2.put(TOPIC_PARTITION_1, new ListOffsetsResultInfo(100L, 0L, null));
    LATEST_OFFSETS_MAP_2.put(TOPIC_PARTITION_2, new ListOffsetsResultInfo(100L, 0L, null));
    LATEST_OFFSETS_MAP_2.put(TOPIC_PARTITION_3, new ListOffsetsResultInfo(100L, 0L, null));
  }

  private static final ConsumerLag CONSUMER_LAG_1 =
      ConsumerLag.builder()
          .setClusterId(CLUSTER_ID)
          .setConsumerGroupId(CONSUMER_GROUP_ID)
          .setTopicName("topic-1")
          .setPartitionId(1)
          .setConsumerId("consumer-1")
          .setInstanceId(Optional.of("instance-1"))
          .setClientId("client-1")
          .setCurrentOffset(0L)
          .setLogEndOffset(100L)
          .build();

  private static final ConsumerLag CONSUMER_LAG_2 =
      ConsumerLag.builder()
          .setClusterId(CLUSTER_ID)
          .setConsumerGroupId(CONSUMER_GROUP_ID)
          .setTopicName("topic-2")
          .setPartitionId(2)
          .setConsumerId("consumer-2")
          .setInstanceId(Optional.of("instance-2"))
          .setClientId("client-2")
          .setCurrentOffset(100L)
          .setLogEndOffset(100L)
          .build();

  private static final ConsumerLag CONSUMER_LAG_3 =
      ConsumerLag.builder()
          .setClusterId(CLUSTER_ID)
          .setConsumerGroupId(CONSUMER_GROUP_ID)
          .setTopicName("topic-3")
          .setPartitionId(3)
          .setConsumerId("consumer-1")
          .setInstanceId(Optional.of("instance-1"))
          .setClientId("client-1")
          .setCurrentOffset(110L)
          .setLogEndOffset(100L)
          .build();

  private static final List<ConsumerLag> CONSUMER_LAG_LIST =
      Arrays.asList(CONSUMER_LAG_1, CONSUMER_LAG_2, CONSUMER_LAG_3);

  @Mock private Admin kafkaAdminClient;

  @Mock private ConsumerGroupManager consumerGroupManager;

  private ConsumerLagManagerImpl consumerLagManager;

  @Mock private ListConsumerGroupOffsetsResult listConsumerGroupOffsetsResult;

  @BeforeEach
  public void setUp() {
    consumerLagManager = new ConsumerLagManagerImpl(kafkaAdminClient, consumerGroupManager);
  }

  @Test
  public void listConsumerLags_returnsConsumerLags() throws Exception {
    expect(consumerGroupManager.getConsumerGroup(CLUSTER_ID, CONSUMER_GROUP_ID))
        .andReturn(completedFuture(Optional.of(CONSUMER_GROUP)));
    expect(
            kafkaAdminClient.listConsumerGroupOffsets(
                eq(CONSUMER_GROUP_ID), anyObject(ListConsumerGroupOffsetsOptions.class)))
        .andReturn(listConsumerGroupOffsetsResult);
    expect(listConsumerGroupOffsetsResult.partitionsToOffsetAndMetadata())
        .andReturn(KafkaFuture.completedFuture(OFFSET_AND_METADATA_MAP));
    final Capture<Map<TopicPartition, OffsetSpec>> capturedOffsetSpec = newCapture();
    final Capture<ListOffsetsOptions> capturedListOffsetsOptions = newCapture();
    expect(
            kafkaAdminClient.listOffsets(
                capture(capturedOffsetSpec), capture(capturedListOffsetsOptions)))
        .andReturn(new ListOffsetsResult(LATEST_OFFSETS_MAP));
    replay(consumerGroupManager, kafkaAdminClient, listConsumerGroupOffsetsResult);
    List<ConsumerLag> consumerLagList =
        consumerLagManager.listConsumerLags(CLUSTER_ID, CONSUMER_GROUP_ID).get();
    assertEquals(OFFSET_AND_METADATA_MAP.keySet(), capturedOffsetSpec.getValue().keySet());
    assertEquals(
        IsolationLevel.READ_COMMITTED, capturedListOffsetsOptions.getValue().isolationLevel());
    assertEquals(CONSUMER_LAG_LIST, consumerLagList);
  }

  @Test
  public void getConsumerLag_returnsConsumerLag() throws Exception {
    expect(consumerGroupManager.getConsumerGroup(CLUSTER_ID, CONSUMER_GROUP_ID))
        .andReturn(completedFuture(Optional.of(CONSUMER_GROUP)));
    expect(
            kafkaAdminClient.listConsumerGroupOffsets(
                eq(CONSUMER_GROUP_ID), anyObject(ListConsumerGroupOffsetsOptions.class)))
        .andReturn(listConsumerGroupOffsetsResult);
    expect(listConsumerGroupOffsetsResult.partitionsToOffsetAndMetadata())
        .andReturn(KafkaFuture.completedFuture(OFFSET_AND_METADATA_MAP));
    final Capture<Map<TopicPartition, OffsetSpec>> capturedOffsetSpec = newCapture();
    final Capture<ListOffsetsOptions> capturedListOffsetsOptions = newCapture();
    expect(
            kafkaAdminClient.listOffsets(
                capture(capturedOffsetSpec), capture(capturedListOffsetsOptions)))
        .andReturn(new ListOffsetsResult(LATEST_OFFSETS_MAP));
    replay(consumerGroupManager, kafkaAdminClient, listConsumerGroupOffsetsResult);

    ConsumerLag consumerLag =
        consumerLagManager.getConsumerLag(CLUSTER_ID, CONSUMER_GROUP_ID, "topic-2", 2).get().get();

    assertEquals(OFFSET_AND_METADATA_MAP.keySet(), capturedOffsetSpec.getValue().keySet());
    assertEquals(
        IsolationLevel.READ_COMMITTED, capturedListOffsetsOptions.getValue().isolationLevel());
    assertEquals(CONSUMER_LAG_2, consumerLag);
  }

  @Test
  public void getConsumerLag_nonExistingPartitionAssignment_returnsConsumerLag() throws Exception {
    final Consumer consumer =
        Consumer.builder()
            .setClusterId(CLUSTER_ID)
            .setConsumerGroupId(CONSUMER_GROUP_ID)
            .setConsumerId("")
            .setClientId("")
            .setHost("11.12.12.14")
            .setAssignedPartitions(
                Collections.singletonList(
                    Partition.create(
                        CLUSTER_ID,
                        /* topicName= */ "topic-1",
                        /* partitionId= */ 1,
                        /* replicas= */ emptyList())))
            .build();

    final ConsumerGroup consumerGroup =
        ConsumerGroup.builder()
            .setClusterId(CLUSTER_ID)
            .setConsumerGroupId(CONSUMER_GROUP_ID)
            .setSimple(true)
            .setPartitionAssignor("org.apache.kafka.clients.consumer.RangeAssignor")
            .setState(State.STABLE)
            .setCoordinator(BROKER_1)
            .setConsumers(Collections.singletonList(consumer))
            .build();

    final ConsumerLag expectedConsumerLag =
        ConsumerLag.builder()
            .setClusterId(CLUSTER_ID)
            .setConsumerGroupId(CONSUMER_GROUP_ID)
            .setTopicName("topic-1")
            .setPartitionId(1)
            .setConsumerId("")
            .setClientId("")
            .setCurrentOffset(0L)
            .setLogEndOffset(100L)
            .build();

    expect(consumerGroupManager.getConsumerGroup(CLUSTER_ID, CONSUMER_GROUP_ID))
        .andReturn(completedFuture(Optional.of(consumerGroup)));
    expect(
            kafkaAdminClient.listConsumerGroupOffsets(
                eq(CONSUMER_GROUP_ID), anyObject(ListConsumerGroupOffsetsOptions.class)))
        .andReturn(listConsumerGroupOffsetsResult);
    expect(listConsumerGroupOffsetsResult.partitionsToOffsetAndMetadata())
        .andReturn(KafkaFuture.completedFuture(OFFSET_AND_METADATA_MAP));
    final Capture<Map<TopicPartition, OffsetSpec>> capturedOffsetSpec = newCapture();
    final Capture<ListOffsetsOptions> capturedListOffsetsOptions = newCapture();
    expect(
            kafkaAdminClient.listOffsets(
                capture(capturedOffsetSpec), capture(capturedListOffsetsOptions)))
        .andReturn(new ListOffsetsResult(LATEST_OFFSETS_MAP));
    replay(consumerGroupManager, kafkaAdminClient, listConsumerGroupOffsetsResult);

    ConsumerLag consumerLag =
        consumerLagManager.getConsumerLag(CLUSTER_ID, CONSUMER_GROUP_ID, "topic-1", 1).get().get();

    assertEquals(OFFSET_AND_METADATA_MAP.keySet(), capturedOffsetSpec.getValue().keySet());
    assertEquals(
        IsolationLevel.READ_COMMITTED, capturedListOffsetsOptions.getValue().isolationLevel());
    assertEquals(expectedConsumerLag, consumerLag);
  }

  @Test
  public void getConsumerLag_nonExistingConsumerLag_returnsEmpty() throws Exception {
    expect(consumerGroupManager.getConsumerGroup(CLUSTER_ID, CONSUMER_GROUP_ID))
        .andReturn(completedFuture(Optional.of(CONSUMER_GROUP)));
    expect(
            kafkaAdminClient.listConsumerGroupOffsets(
                eq(CONSUMER_GROUP_ID), anyObject(ListConsumerGroupOffsetsOptions.class)))
        .andReturn(listConsumerGroupOffsetsResult);
    expect(listConsumerGroupOffsetsResult.partitionsToOffsetAndMetadata())
        .andReturn(KafkaFuture.completedFuture(OFFSET_AND_METADATA_MAP));
    final Capture<Map<TopicPartition, OffsetSpec>> capturedOffsetSpec = newCapture();
    final Capture<ListOffsetsOptions> capturedListOffsetsOptions = newCapture();
    expect(
            kafkaAdminClient.listOffsets(
                capture(capturedOffsetSpec), capture(capturedListOffsetsOptions)))
        .andReturn(new ListOffsetsResult(LATEST_OFFSETS_MAP));
    replay(consumerGroupManager, kafkaAdminClient, listConsumerGroupOffsetsResult);

    Optional<ConsumerLag> consumerLag =
        consumerLagManager.getConsumerLag(CLUSTER_ID, CONSUMER_GROUP_ID, "topic-1", 2).get();
    assertEquals(OFFSET_AND_METADATA_MAP.keySet(), capturedOffsetSpec.getValue().keySet());
    assertEquals(
        IsolationLevel.READ_COMMITTED, capturedListOffsetsOptions.getValue().isolationLevel());
    assertEquals(consumerLag, Optional.empty());
  }

  @Test
  public void getConsumerLag_nonExistingConsumerGroup_throwsNotFound() throws Exception {
    expect(consumerGroupManager.getConsumerGroup(CLUSTER_ID, "foo"))
        .andReturn(completedFuture(Optional.empty()));
    replay(consumerGroupManager);

    try {
      consumerLagManager.getConsumerLag(CLUSTER_ID, "foo", "topic-1", 1).get();
      fail();
    } catch (ExecutionException e) {
      assertEquals(NotFoundException.class, e.getCause().getClass());
    }
  }

  @Test
  public void listConsumerLag_nonExistingConsumerGroup_throwsNotFound() throws Exception {
    expect(consumerGroupManager.getConsumerGroup(CLUSTER_ID, "foo"))
        .andReturn(completedFuture(Optional.empty()));
    replay(consumerGroupManager);

    try {
      consumerLagManager.listConsumerLags(CLUSTER_ID, "foo").get();
      fail();
    } catch (ExecutionException e) {
      assertEquals(NotFoundException.class, e.getCause().getClass());
    }
  }
}
