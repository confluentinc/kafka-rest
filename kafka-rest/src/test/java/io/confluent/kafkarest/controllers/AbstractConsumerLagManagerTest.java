package io.confluent.kafkarest.controllers;

import java.util.HashMap;
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

  private static class TestAbstractConsumerLagManager extends AbstractConsumerLagManager {

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
    expect(kafkaAdminClient.listConsumerGroupOffsets(
        eq(CONSUMER_GROUP_ID), anyObject(ListConsumerGroupOffsetsOptions.class)))
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
        capture(capturedOffsetSpec), capture(capturedListOffsetsOptions)))
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
}
