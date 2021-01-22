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
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.createMockBuilder;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import io.confluent.kafkarest.controllers.AbstractConsumerLagManager.MemberId;
import io.confluent.kafkarest.entities.Broker;
import io.confluent.kafkarest.entities.Consumer;
import io.confluent.kafkarest.entities.ConsumerGroup;
import io.confluent.kafkarest.entities.ConsumerGroup.State;
import io.confluent.kafkarest.entities.ConsumerGroupLag;
import io.confluent.kafkarest.entities.Partition;
import io.confluent.kafkarest.entities.Topic;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsOptions;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.easymock.EasyMockRule;
import org.easymock.IMockBuilder;
import org.easymock.Mock;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ConsumerGroupLagManagerImplTest {

  private static final String CLUSTER_ID = "cluster-1";
  private static final String CONSUMER_GROUP_ID = "consumer_group_1";

  private static final ConsumerGroupLag CONSUMER_GROUP_LAG =
      ConsumerGroupLag.builder()
      .setClusterId(CLUSTER_ID)
      .setConsumerGroupId(CONSUMER_GROUP_ID)
      .setMaxLag(100L)
      .setTotalLag(90L)
      .setMaxLagConsumerId("consumer-1")
      .setMaxLagClientId("client-1")
      .setMaxLagTopicName("topic-1")
      .setMaxLagPartitionId(1)
      .build();

  private static final Broker BROKER_1 =
      Broker.create(
          CLUSTER_ID,
          /* brokerId= */ 1,
          /* host= */ "1.2.3.4",
          /* port= */ 1000,
          /* rack= */ null);

  private static final TopicPartition TOPIC_PARTITION_1 =
      new TopicPartition("topic-1", 1);

  private static final TopicPartition TOPIC_PARTITION_2 =
      new TopicPartition("topic-2", 2);

  private static final TopicPartition TOPIC_PARTITION_3 =
      new TopicPartition("topic-3", 3);

  private static final Consumer CONSUMER_1 =
      Consumer.builder()
          .setClusterId(CLUSTER_ID)
          .setConsumerGroupId("consumer-group-1")
          .setConsumerId("consumer-1")
          .setClientId("client-1")
          .setInstanceId("instance-1")
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
          .setConsumerGroupId("consumer-group-1")
          .setConsumerId("consumer-2")
          .setClientId("client-2")
          .setInstanceId("instance-2")
          .setHost("11.12.12.14")
          .setAssignedPartitions(
              Collections.singletonList(
                  Partition.create(
                      CLUSTER_ID,
                      /* topicName= */ "topic-2",
                      /* partitionId= */ 2,
                      /* replicas= */ emptyList())))
          .build();

  private static final Map<TopicPartition, OffsetAndMetadata> OFFSET_AND_METADATA_MAP;
  static {
    OFFSET_AND_METADATA_MAP = new HashMap<>();
    OFFSET_AND_METADATA_MAP.put(TOPIC_PARTITION_1, new OffsetAndMetadata(0));
    OFFSET_AND_METADATA_MAP.put(TOPIC_PARTITION_2, new OffsetAndMetadata(100));
    OFFSET_AND_METADATA_MAP.put(TOPIC_PARTITION_3, new OffsetAndMetadata(110));
  }

//  private static final ListConsumerGroupOffsetsResult CURRENT_OFFSETS =
//      new ListConsumerGroupOffsetsResult(KafkaFuture.completedFuture(OFFSET_AND_METADATA_MAP));

  private static final Map<TopicPartition, ListOffsetsResultInfo> LATEST_OFFSETS;
  static {
    LATEST_OFFSETS = new HashMap<>();
    LATEST_OFFSETS.put(TOPIC_PARTITION_1, new ListOffsetsResultInfo(100L, 0L, null));
    LATEST_OFFSETS.put(TOPIC_PARTITION_2, new ListOffsetsResultInfo(100L, 0L, null));
    LATEST_OFFSETS.put(TOPIC_PARTITION_3, new ListOffsetsResultInfo(100L, 0L, null));
  }

  private static final Map<TopicPartition, MemberId> MEMBER_ID_MAP;
  static {
    MEMBER_ID_MAP = new HashMap<>();
    MEMBER_ID_MAP.put(
        TOPIC_PARTITION_1,
        MemberId.builder()
            .setConsumerId("consumer-1")
            .setClientId("client-1")
            .setInstanceId("instance-1")
            .build());
    MEMBER_ID_MAP.put(
        TOPIC_PARTITION_2,
        MemberId.builder()
            .setConsumerId("consumer-2")
            .setClientId("client-2")
            .setInstanceId("instance-2")
            .build());
    MEMBER_ID_MAP.put(
        TOPIC_PARTITION_3,
        MemberId.builder()
            .setConsumerId("consumer-1")
            .setClientId("client-1")
            .setInstanceId("instance-1")
            .build());
  }

  private static final ConsumerGroup CONSUMER_GROUP =
      ConsumerGroup.builder()
          .setClusterId(CLUSTER_ID)
          .setConsumerGroupId("consumer-group-1")
          .setSimple(true)
          .setPartitionAssignor("org.apache.kafka.clients.consumer.RangeAssignor")
          .setState(State.STABLE)
          .setCoordinator(BROKER_1)
          .setConsumers(Arrays.asList(CONSUMER_1, CONSUMER_2))
          .build();

  @Rule
  public final EasyMockRule mocks = new EasyMockRule(this);

  @Mock
  private ConsumerGroupManager consumerGroupManager;

  @Mock
  private Admin kafkaAdminClient;

  private ConsumerGroupLagManagerImpl consumerGroupLagManager;

  @Before
  public void setUp() {
//    consumerGroupLagManager =
//        new ConsumerGroupLagManagerImpl(kafkaAdminClient, consumerGroupManager);
    consumerGroupLagManager =
        createMockBuilder(ConsumerGroupLagManagerImpl.class)
            .addMockedMethod("getMemberIds")
            .addMockedMethod("getCurrentOffsets")
            .addMockedMethod("getLatestOffsets")
            .addMockedMethod("getCurrentOffset")
            .addMockedMethod("getOffset")
            .createMock();
  }

  @Test
  public void getConsumerGroupLag_returnsConsumerGroupLag() throws Exception {
    expect(consumerGroupManager.getConsumerGroup(CLUSTER_ID, CONSUMER_GROUP_ID))
        .andReturn(completedFuture(Optional.of(CONSUMER_GROUP)));
//    expect(kafkaAdminClient.listConsumerGroupOffsets(CONSUMER_GROUP_ID, new ListConsumerGroupOffsetsOptions()))
//        .andReturn(CURRENT_OFFSETS);
    expect(consumerGroupLagManager.getCurrentOffsets(CONSUMER_GROUP_ID))
        .andReturn(CompletableFuture.completedFuture(OFFSET_AND_METADATA_MAP));
    expect(consumerGroupLagManager.getLatestOffsets(OFFSET_AND_METADATA_MAP))
        .andReturn(CompletableFuture.completedFuture(LATEST_OFFSETS));
    expect(consumerGroupLagManager.getMemberIds(CONSUMER_GROUP))
        .andReturn(MEMBER_ID_MAP);
    replay(consumerGroupManager, consumerGroupLagManager);

    ConsumerGroupLag consumerGroupLag =
        consumerGroupLagManager.getConsumerGroupLag(CLUSTER_ID, CONSUMER_GROUP_ID).get().get();
    assertEquals(CONSUMER_GROUP_LAG, consumerGroupLag);
  }
//
//  @Test
//  public void getConsumerGroupLag_nonExistingConsumerGroupLag_returnsEmpty() throws Exception {
//    expect(consumerOffsetsDao.getConsumerGroupOffsets(
//        CLUSTER_ID, CONSUMER_GROUP_ID, IsolationLevel.READ_COMMITTED))
//        .andReturn(null);
//    replay(consumerOffsetsDao);
//
//    Optional<ConsumerGroupLag> consumerGroupLag = consumerGroupLagManager.getConsumerGroupLag(
//        CLUSTER_ID, CONSUMER_GROUP_ID).get();
//
//    assertFalse(consumerGroupLag.isPresent());
//  }
}
