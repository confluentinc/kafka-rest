package io.confluent.kafkarest.controllers;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.confluent.kafkarest.entities.ConsumerGroupLag;
import java.time.Duration;
import java.util.Optional;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.DescribeConsumerGroupsOptions;
import org.apache.kafka.clients.admin.DescribeConsumerGroupsResult;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.ListConsumerGroupsOptions;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsOptions;
import org.apache.kafka.clients.admin.ListConsumerGroupsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.MemberAssignment;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.ConsumerGroupState;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.junit.Before;
import org.junit.Test;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.easymock.EasyMock.*;
import static org.junit.Assert.assertEquals;

public class AbstractConsumerLagManagerTest {

  private static final Duration DEFAULT_METADATA_TIMEOUT = Duration.ofSeconds(15);
  private AdminClient adminClient;
  private String CLUSTER_ID = "cluster-1";

  @Before
  public void setup() {
    adminClient =  createMock(AdminClient.class);
  }

//  @Test
//  public void testGetConsumerGroups() throws Throwable {
//    ListConsumerGroupsResult listConsumerGroupsResult = mock(ListConsumerGroupsResult.class);
//    expect(adminClient.listConsumerGroups(anyObject(ListConsumerGroupsOptions.class))).andReturn(listConsumerGroupsResult).once();
//    Collection<ConsumerGroupListing> consumerGroups = Lists.newArrayList();
//    expect(listConsumerGroupsResult.all()).andReturn(KafkaFuture.completedFuture(consumerGroups));
//    expectLastCall().once();
//    replay(adminClient, listConsumerGroupsResult);
//    AbstractConsumerLagManager dao = new AbstractConsumerLagManager(adminClient, DEFAULT_METADATA_TIMEOUT);
//    dao.getConsumerGroups();
//    verify(adminClient, listConsumerGroupsResult);
//  }

//  @Test
//  public void testGetAllConsumerGroupDescriptions() throws Throwable {
//
//    Collection<String> consumerGroupIds = ImmutableList.of("consumerGroup1", "consumerGroup2", "consumerGroup3");
//    ConsumerGroupDescription desc1 = new ConsumerGroupDescription("consumerGroup1", true, getMemberDescriptions(), "something",
//        ConsumerGroupState.EMPTY, Node.noNode());
//    ConsumerGroupDescription desc2 = new ConsumerGroupDescription("consumerGroup2", true, getMemberDescriptions(), "something",
//        ConsumerGroupState.STABLE, Node.noNode());
//    ConsumerGroupDescription desc3 = new ConsumerGroupDescription("consumerGroup3", true, null, "something",
//        ConsumerGroupState.STABLE, Node.noNode());
//
//    DescribeConsumerGroupsResult describeConsumerGroupsResult = mock(DescribeConsumerGroupsResult.class);
//    expect(adminClient.describeConsumerGroups(eq(consumerGroupIds), anyObject(DescribeConsumerGroupsOptions.class))).andReturn(describeConsumerGroupsResult).once();
//    Map<String, KafkaFuture<ConsumerGroupDescription>> futuresMap = ImmutableMap.of(
//        "consumerGroup1", KafkaFuture.completedFuture(desc1),
//        "consumerGroup2", KafkaFuture.completedFuture(desc2),
//        "consumerGroup3", KafkaFuture.completedFuture(desc3)
//    );
//    expect(describeConsumerGroupsResult.describedGroups()).andReturn(futuresMap);
//    expectLastCall().once();
//
//    replay(adminClient, describeConsumerGroupsResult);
//    AbstractConsumerLagManager dao = new AbstractConsumerLagManager(adminClient, DEFAULT_METADATA_TIMEOUT);
//    assertEquals(ImmutableMap.of("consumerGroup1", desc1, "consumerGroup2", desc2, "consumerGroup3", desc3), dao.getAllConsumerGroupDescriptions(consumerGroupIds));
//    verify(adminClient, describeConsumerGroupsResult);
//  }

//  @Test
//  public void testGetCurrentOffsets() throws Throwable {
//
//    final String consumerGroupId = "consumerGroup1";
//    ListConsumerGroupOffsetsResult listConsumerGroupOffsetsResult = mock(ListConsumerGroupOffsetsResult.class);
//    expect(adminClient.listConsumerGroupOffsets(eq(consumerGroupId), anyObject(ListConsumerGroupOffsetsOptions.class))).andReturn(listConsumerGroupOffsetsResult).once();
//    Map<TopicPartition, OffsetAndMetadata> tpMetaData = Maps.newHashMap();
//    expect(listConsumerGroupOffsetsResult.partitionsToOffsetAndMetadata()).andReturn(KafkaFuture.completedFuture(tpMetaData));
//    expectLastCall().once();
//
//    replay(adminClient, listConsumerGroupOffsetsResult);
//    AbstractConsumerLagManager dao = new AbstractConsumerLagManager(adminClient, DEFAULT_METADATA_TIMEOUT);
//    dao.getCurrentOffsets(consumerGroupId);
//    verify(adminClient, listConsumerGroupOffsetsResult);
//  }

  private ListOffsetsResult.ListOffsetsResultInfo listOffsetResult(long offset) {
    return new ListOffsetsResult.ListOffsetsResultInfo(offset, 0, Optional.empty());
  }

//  @Test
//  public void getConsumerGroupOffsets_returnsCorrectLagSummary() throws Throwable {
//    AbstractConsumerLagManager dao = new AbstractConsumerLagManager(adminClient, DEFAULT_METADATA_TIMEOUT);
//
//    ConsumerGroupDescription consumerGroupDescription = new ConsumerGroupDescription("consumerGroup1", true, getMemberDescriptions(), "something",
//        ConsumerGroupState.STABLE, Node.noNode());
//
//    Map<TopicPartition, OffsetAndMetadata> currentOffsets = ImmutableMap.of(
//        new TopicPartition("topic1", 1), new OffsetAndMetadata(50, null),
//        new TopicPartition("topic1", 2), new OffsetAndMetadata(0, null),
//        new TopicPartition("topic3", 1), new OffsetAndMetadata(100, null),
//        new TopicPartition("topic99", 1), new OffsetAndMetadata(99, null)
//    );
//
//    Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> endOffsets = ImmutableMap.of(
//        new TopicPartition("topic1", 1), listOffsetResult(75L),
//        new TopicPartition("topic1", 2), listOffsetResult(25L),
//        new TopicPartition("topic3", 1), listOffsetResult(200L)
//    );
//
//    ConsumerGroupLag lag= dao.getConsumerGroupOffsets(
//        consumerGroupDescription, currentOffsets, endOffsets).setClusterId(CLUSTER_ID).build();
//    assertEquals("cluster-1", lag.getClusterId());
//    assertEquals("consumerGroup1", lag.getConsumerGroupId());
//    assertEquals(100, (long) lag.getMaxLag());
//    assertEquals(150, (long) lag.getTotalLag());
//    assertEquals("consumer2", lag.getMaxLagConsumerId());
//    assertEquals("client2", lag.getMaxLagClientId());
//    assertEquals("topic3", lag.getMaxLagTopicName());
//    assertEquals(1, (long) lag.getMaxLagPartitionId());
//
//    ConsumerGroupDescription consumerGroupDescription2 = new ConsumerGroupDescription("consumerGroup2", true, null, "something",
//        ConsumerGroupState.STABLE, Node.noNode());
//
//    Map<TopicPartition, OffsetAndMetadata> currentOffsets2 = ImmutableMap.of(
//        new TopicPartition("topic1", 1), new OffsetAndMetadata(50, null),
//        new TopicPartition("topic1", 2), new OffsetAndMetadata(0, null),
//        new TopicPartition("topic3", 1), new OffsetAndMetadata(100, null),
//        new TopicPartition("topic99", 1), new OffsetAndMetadata(99, null)
//    );
//
//    Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> endOffsets2 = ImmutableMap.of(
//        new TopicPartition("topic1", 1), listOffsetResult(75L),
//        new TopicPartition("topic1", 2), listOffsetResult(25L),
//        new TopicPartition("topic3", 1), listOffsetResult(99L)
//    );
//
//    ConsumerGroupLag lag2 = dao.getConsumerGroupOffsets(
//        consumerGroupDescription2, currentOffsets2, endOffsets2).setClusterId(CLUSTER_ID).build();
//    assertEquals("cluster-1", lag2.getClusterId());
//    assertEquals("consumerGroup2", lag2.getConsumerGroupId());
//    assertEquals(25, (long) lag2.getMaxLag());
//    assertEquals(49, (long) lag2.getTotalLag());
//    assertEquals("", lag2.getMaxLagConsumerId());
//    assertEquals("", lag2.getMaxLagClientId());
//    assertEquals("topic1", lag2.getMaxLagTopicName());
//    assertEquals(1, (long) lag2.getMaxLagPartitionId());
//  }

  private List<MemberDescription> getMemberDescriptions() {
    MemberAssignment assignment1 = new MemberAssignment(ImmutableSet.of(
        new TopicPartition("topic1", 1),
        new TopicPartition("topic1", 2),
        new TopicPartition("topic2", 1),
        new TopicPartition("topic3", 0)
    ));

    MemberAssignment assignment2 = new MemberAssignment(ImmutableSet.of(
        new TopicPartition("topic1", 0),
        new TopicPartition("topic2", 1),
        new TopicPartition("topic3", 1),
        new TopicPartition("topic4", 0)
    ));

    return ImmutableList.of(
        new MemberDescription("consumer1", "client1", "host1", assignment1),
        new MemberDescription("consumer2", "client2", "host1", assignment2)
    );
  }
}
