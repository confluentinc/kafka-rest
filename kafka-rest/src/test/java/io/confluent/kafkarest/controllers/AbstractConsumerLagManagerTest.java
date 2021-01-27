package io.confluent.kafkarest.controllers;

import io.confluent.kafkarest.entities.Broker;
import java.util.HashMap;
import javax.annotation.Nullable;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsOptions;
import org.apache.kafka.clients.admin.ListOffsetsOptions;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.easymock.Capture;
import org.easymock.EasyMockRule;
import org.easymock.Mock;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.Map;

import static org.easymock.EasyMock.*;
import static org.junit.Assert.assertEquals;

public class AbstractConsumerLagManagerTest {

  private static final String CLUSTER_ID = "cluster-1";

  private static final Broker BROKER_1 =
      Broker.create(
          CLUSTER_ID,
          /* brokerId= */ 1,
          /* host= */ "1.2.3.4",
          /* port= */ 1000,
          /* rack= */ null);

  private static final String CONSUMER_GROUP_ID = "consumer-group-1";

  private static final TopicPartition TOPIC_PARTITION_1 =
      new TopicPartition("topic-1", 1);

  private static final TopicPartition TOPIC_PARTITION_2 =
      new TopicPartition("topic-2", 2);

  private static final TopicPartition TOPIC_PARTITION_3 =
      new TopicPartition("topic-3", 3);

  private static final Map<TopicPartition, OffsetAndMetadata> OFFSET_AND_METADATA_MAP;
  static {
    OFFSET_AND_METADATA_MAP = new HashMap<>();
    OFFSET_AND_METADATA_MAP.put(TOPIC_PARTITION_1, new OffsetAndMetadata(0));
    OFFSET_AND_METADATA_MAP.put(TOPIC_PARTITION_2, new OffsetAndMetadata(100));
    OFFSET_AND_METADATA_MAP.put(TOPIC_PARTITION_3, new OffsetAndMetadata(110));
  }

  private static final Map<TopicPartition, KafkaFuture<ListOffsetsResultInfo>> LATEST_OFFSETS_MAP =
      new HashMap<>();

  @Rule
  public final EasyMockRule mocks = new EasyMockRule(this);

  @Mock
  private Admin kafkaAdminClient;

  @Mock
  private ListConsumerGroupOffsetsResult listConsumerGroupOffsetsResult;

  private class TestAbstractConsumerLagManager extends AbstractConsumerLagManager {

    TestAbstractConsumerLagManager(final Admin kafkaAdminClient) {
      super(kafkaAdminClient);
    }
  }

  private TestAbstractConsumerLagManager testAbstractConsumerLagManager;

  @Before
  public void setup() {
    testAbstractConsumerLagManager = new TestAbstractConsumerLagManager(kafkaAdminClient);
  }

  @Test
  public void testGetCurrentOffsets() {
    expect(kafkaAdminClient.listConsumerGroupOffsets(eq(CONSUMER_GROUP_ID), anyObject(ListConsumerGroupOffsetsOptions.class)))
        .andReturn(listConsumerGroupOffsetsResult)
        .once();
    expect(listConsumerGroupOffsetsResult.partitionsToOffsetAndMetadata())
        .andReturn(KafkaFuture.completedFuture(OFFSET_AND_METADATA_MAP));
    expectLastCall().once();

    replay(kafkaAdminClient, listConsumerGroupOffsetsResult);
    testAbstractConsumerLagManager.getCurrentOffsets(CONSUMER_GROUP_ID);
    verify(kafkaAdminClient, listConsumerGroupOffsetsResult);
  }

  @Test
  public void testLatestOffsets() {
    final Capture<Map<TopicPartition, OffsetSpec>> capturedOffsetSpec = newCapture();
    final Capture<ListOffsetsOptions> capturedListOffsetsOptions = newCapture();
    expect(kafkaAdminClient.listOffsets(
        capture(capturedOffsetSpec),
        capture(capturedListOffsetsOptions)))
        .andReturn(new ListOffsetsResult(LATEST_OFFSETS_MAP))
        .once();

    replay(kafkaAdminClient);
    testAbstractConsumerLagManager.getLatestOffsets(OFFSET_AND_METADATA_MAP);

    verify(kafkaAdminClient);
    assertEquals(OFFSET_AND_METADATA_MAP.keySet(), capturedOffsetSpec.getValue().keySet());
    assertEquals(
        IsolationLevel.READ_COMMITTED,
        capturedListOffsetsOptions.getValue().isolationLevel());
  }

//  @Test
//  public void testGetMemberIds() {
//    final Consumer CONSUMER_1 =
//        Consumer.builder()
//            .setClusterId(CLUSTER_ID)
//            .setConsumerGroupId(CONSUMER_GROUP_ID)
//            .setConsumerId("consumer-1")
//            .setInstanceId("instance-1")
//            .setClientId("client-1")
//            .setHost("11.12.12.14")
//            .setAssignedPartitions(
//                Arrays.asList(
//                    Partition.create(
//                        CLUSTER_ID,
//                        /* topicName= */ "topic-1",
//                        /* partitionId= */ 1,
//                        /* replicas= */ emptyList()),
//                    Partition.create(
//                        CLUSTER_ID,
//                        /* topicName= */ "topic-3",
//                        /* partitionId= */ 3,
//                        /* replicas= */ emptyList())))
//            .build();
//
//    final Consumer CONSUMER_2 =
//        Consumer.builder()
//            .setClusterId(CLUSTER_ID)
//            .setConsumerGroupId(CONSUMER_GROUP_ID)
//            .setConsumerId("consumer-2")
//            .setInstanceId("instance-2")
//            .setClientId("client-2")
//            .setHost("11.12.12.14")
//            .setAssignedPartitions(
//                Collections.singletonList(
//                    Partition.create(
//                        CLUSTER_ID,
//                        /* topicName= */ "topic-2",
//                        /* partitionId= */ 2,
//                        /* replicas= */ emptyList())))
//            .build();
//
//    final ConsumerGroup CONSUMER_GROUP =
//        ConsumerGroup.builder()
//            .setClusterId(CLUSTER_ID)
//            .setConsumerGroupId(CONSUMER_GROUP_ID)
//            .setSimple(true)
//            .setPartitionAssignor("org.apache.kafka.clients.consumer.RoundRobinAssignor")
//            .setState(State.STABLE)
//            .setCoordinator(BROKER_1)
//            .setConsumers(Arrays.asList(CONSUMER_1, CONSUMER_2))
//            .build();
//
//    // using round robin partition assignor
//    Map<TopicPartition, MemberId> expectedTpMemberIds = new HashMap<>();
//    expectedTpMemberIds.put(TOPIC_PARTITION_1, createMemberId("consumer-1", "instance-1", "client-1"));
//    expectedTpMemberIds.put(TOPIC_PARTITION_2, createMemberId("consumer-2", "instance-2", "client-2"));
//    expectedTpMemberIds.put(TOPIC_PARTITION_3, createMemberId("consumer-1", "instance-1", "client-1"));
//
//    assertEquals(expectedTpMemberIds, testAbstractConsumerLagManager.getMemberIds(CONSUMER_GROUP));
//  }

//  @Test
//  public void testGetMemberIds_nullInstances_returnsMemberIds() {
//    final Consumer CONSUMER_1 =
//        Consumer.builder()
//            .setClusterId(CLUSTER_ID)
//            .setConsumerGroupId(CONSUMER_GROUP_ID)
//            .setConsumerId("consumer-1")
//            .setInstanceId(null)
//            .setClientId("client-1")
//            .setHost("11.12.12.14")
//            .setAssignedPartitions(
//                Arrays.asList(
//                    Partition.create(
//                        CLUSTER_ID,
//                        /* topicName= */ "topic-1",
//                        /* partitionId= */ 1,
//                        /* replicas= */ emptyList()),
//                    Partition.create(
//                        CLUSTER_ID,
//                        /* topicName= */ "topic-3",
//                        /* partitionId= */ 3,
//                        /* replicas= */ emptyList())))
//            .build();
//
//    final ConsumerGroup CONSUMER_GROUP =
//        ConsumerGroup.builder()
//            .setClusterId(CLUSTER_ID)
//            .setConsumerGroupId(CONSUMER_GROUP_ID)
//            .setSimple(true)
//            .setPartitionAssignor("org.apache.kafka.clients.consumer.RoundRobinAssignor")
//            .setState(State.STABLE)
//            .setCoordinator(BROKER_1)
//            .setConsumers(Collections.singletonList(CONSUMER_1))
//            .build();
//
//    Map<TopicPartition, MemberId> expectedTpMemberIds = new HashMap<>();
//    expectedTpMemberIds.put(TOPIC_PARTITION_1, createMemberId("consumer-1", null, "client-1"));
//    expectedTpMemberIds.put(TOPIC_PARTITION_3, createMemberId("consumer-1", null, "client-1"));
//    assertEquals(expectedTpMemberIds, testAbstractConsumerLagManager.getMemberIds(CONSUMER_GROUP));
//  }

//  private MemberId createMemberId(String consumer, @Nullable String instance, String client) {
//    return MemberId.builder()
//        .setConsumerId(consumer)
//        .setInstanceId(instance)
//        .setClientId(client)
//        .build();
//  }
}
