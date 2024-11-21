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
import io.confluent.kafkarest.entities.ConsumerGroupLagSummary;
import io.confluent.kafkarest.entities.Partition;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
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
public class ConsumerGroupLagSummaryManagerImplTest {

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

  private static final ConsumerGroupLagSummary CONSUMER_GROUP_LAG_SUMMARY =
      ConsumerGroupLagSummary.builder()
          .setClusterId(CLUSTER_ID)
          .setConsumerGroupId(CONSUMER_GROUP_ID)
          .setMaxLagConsumerId("consumer-1")
          .setMaxLagClientId("client-1")
          .setMaxLagInstanceId(Optional.of("instance-1"))
          .setMaxLagTopicName("topic-1")
          .setMaxLagPartitionId(1)
          .setMaxLag(100L)
          .setTotalLag(100L) // negative lag is ignored
          .build();

  @Mock private ConsumerGroupManager consumerGroupManager;

  @Mock private Admin kafkaAdminClient;

  private ConsumerGroupLagSummaryManagerImpl consumerGroupLagSummaryManager;

  @Mock private ListConsumerGroupOffsetsResult listConsumerGroupOffsetsResult;

  @BeforeEach
  public void setUp() {
    consumerGroupLagSummaryManager =
        new ConsumerGroupLagSummaryManagerImpl(kafkaAdminClient, consumerGroupManager);
  }

  @Test
  public void getConsumerGroupLagSummary_returnsConsumerGroupLagSummary() throws Exception {
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
    ConsumerGroupLagSummary consumerGroupLagSummary =
        consumerGroupLagSummaryManager
            .getConsumerGroupLagSummary(CLUSTER_ID, CONSUMER_GROUP_ID)
            .get()
            .get();
    assertEquals(OFFSET_AND_METADATA_MAP.keySet(), capturedOffsetSpec.getValue().keySet());
    assertEquals(
        IsolationLevel.READ_COMMITTED, capturedListOffsetsOptions.getValue().isolationLevel());
    assertEquals(CONSUMER_GROUP_LAG_SUMMARY, consumerGroupLagSummary);
  }

  @Test
  public void getConsumerGroupLagSummary_nonExistingConsumerGroup_throwsNotFound()
      throws Exception {
    expect(consumerGroupManager.getConsumerGroup(CLUSTER_ID, CONSUMER_GROUP_ID))
        .andReturn(completedFuture(Optional.empty()));
    replay(consumerGroupManager);

    try {
      consumerGroupLagSummaryManager
          .getConsumerGroupLagSummary(CLUSTER_ID, CONSUMER_GROUP_ID)
          .get();
      fail();
    } catch (ExecutionException e) {
      assertEquals(NotFoundException.class, e.getCause().getClass());
    }
  }
}
