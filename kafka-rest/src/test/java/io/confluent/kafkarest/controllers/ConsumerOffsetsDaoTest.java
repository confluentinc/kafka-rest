package io.confluent.kafkarest.controllers;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
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

public class ConsumerOffsetsDaoTest {

  private static final Duration DEFAULT_METADATA_TIMEOUT = Duration.ofSeconds(15);
  private AdminClient adminClient;

  @Before
  public void setup() {
    adminClient =  createMock(AdminClient.class);
  }

  @Test
  public void testGetConsumerGroups() throws Throwable {
    ListConsumerGroupsResult lcgr = mock(ListConsumerGroupsResult.class);
    expect(adminClient.listConsumerGroups(anyObject(ListConsumerGroupsOptions.class))).andReturn(lcgr).once();
    Collection<ConsumerGroupListing> consumerGroups = Lists.newArrayList();
    expect(lcgr.all()).andReturn(KafkaFuture.completedFuture(consumerGroups));
    expectLastCall().once();
    replay(adminClient, lcgr);
    ConsumerOffsetsDao dao = new ConsumerOffsetsDao(adminClient, DEFAULT_METADATA_TIMEOUT);
    dao.getConsumerGroups();
    verify(adminClient, lcgr);
  }

  @Test
  public void testGetAllConsumerGroupDescriptions() throws Throwable {

    Collection<String> consumerGroupIds = ImmutableList.of("cg1", "cg2");
    ConsumerGroupDescription desc1 = new ConsumerGroupDescription("cg1", true, getMemberDescriptions(), "something",
        ConsumerGroupState.EMPTY, Node.noNode());
    ConsumerGroupDescription desc2 = new ConsumerGroupDescription("cg2", true, getMemberDescriptions(), "something",
        ConsumerGroupState.STABLE, Node.noNode());
    ConsumerGroupDescription desc3 = new ConsumerGroupDescription("cg2", true, null, "something",
        ConsumerGroupState.STABLE, Node.noNode());

    DescribeConsumerGroupsResult dcgr = mock(DescribeConsumerGroupsResult.class);
    expect(adminClient.describeConsumerGroups(eq(consumerGroupIds), anyObject(DescribeConsumerGroupsOptions.class))).andReturn(dcgr).once();
    Map<String, KafkaFuture<ConsumerGroupDescription>> futuresMap = ImmutableMap.of(
        "cg1", KafkaFuture.completedFuture(desc1),
        "cg2", KafkaFuture.completedFuture(desc2),
        "cg3", KafkaFuture.completedFuture(desc3)
    );
    expect(dcgr.describedGroups()).andReturn(futuresMap);
    expectLastCall().once();

    replay(adminClient, dcgr);
    ConsumerOffsetsDao dao = new ConsumerOffsetsDao(adminClient, DEFAULT_METADATA_TIMEOUT);
    assertEquals(ImmutableMap.of("cg1", desc1, "cg2", desc2, "cg3", desc3), dao.getAllConsumerGroupDescriptions(consumerGroupIds));
    verify(adminClient, dcgr);
  }

  @Test
  public void testGetCurrentOffsets() throws Throwable {

    final String consumerGroupId = "cg1";
    ListConsumerGroupOffsetsResult lcgor = mock(ListConsumerGroupOffsetsResult.class);
    expect(adminClient.listConsumerGroupOffsets(eq(consumerGroupId), anyObject(ListConsumerGroupOffsetsOptions.class))).andReturn(lcgor).once();
    Map<TopicPartition, OffsetAndMetadata> tpMetaData = Maps.newHashMap();
    expect(lcgor.partitionsToOffsetAndMetadata()).andReturn(KafkaFuture.completedFuture(tpMetaData));
    expectLastCall().once();

    replay(adminClient, lcgor);
    ConsumerOffsetsDao dao = new ConsumerOffsetsDao(adminClient, DEFAULT_METADATA_TIMEOUT);
    dao.getCurrentOffsets(consumerGroupId);
    verify(adminClient, lcgor);
  }

  private ListOffsetsResult.ListOffsetsResultInfo listOffsetResult(long offset) {
    return new ListOffsetsResult.ListOffsetsResultInfo(offset, 0, Optional.empty());
  }

  @Test
  public void testGetConsumerGroupOffsets() throws Throwable {
    ConsumerOffsetsDao dao = new ConsumerOffsetsDao(adminClient, DEFAULT_METADATA_TIMEOUT);

    ConsumerGroupDescription cgDesc = new ConsumerGroupDescription("cg1", true, getMemberDescriptions(), "something",
        ConsumerGroupState.STABLE, Node.noNode());

    Map<TopicPartition, OffsetAndMetadata> currentOffsets = ImmutableMap.of(
        new TopicPartition("topic1", 1), new OffsetAndMetadata(50, null),
        new TopicPartition("topic1", 2), new OffsetAndMetadata(0, null),
        new TopicPartition("topic3", 1), new OffsetAndMetadata(100, null),
        new TopicPartition("topic99", 1), new OffsetAndMetadata(99, null)
    );
    Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> beginningOffsets = ImmutableMap.of(
        new TopicPartition("topic1", 1), listOffsetResult(0L),
        new TopicPartition("topic1", 2), listOffsetResult(0L),
        new TopicPartition("topic3", 1), listOffsetResult(0L)
    );

    Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> endOffsets = ImmutableMap.of(
        new TopicPartition("topic1", 1), listOffsetResult(75L),
        new TopicPartition("topic1", 2), listOffsetResult(25L),
        new TopicPartition("topic3", 1), listOffsetResult(99L)
    );

    ConsumerGroupOffsets offsets = dao.getConsumerGroupOffsets(
        cgDesc, currentOffsets, beginningOffsets, endOffsets
    );
    assertEquals("cg1", offsets.getConsumerGroupId());
    assertEquals(150, offsets.getSumCurrentOffset());
    assertEquals(199, offsets.getSumEndOffset());
    assertEquals(49, offsets.getTotalLag());
    assertEquals(2, offsets.getNumConsumers());
    assertEquals(2, offsets.consumerGroupOffsets.size());

    ConsumerGroupDescription cgDesc2 = new ConsumerGroupDescription("cg2", true, null, "something",
        ConsumerGroupState.STABLE, Node.noNode());

    Map<TopicPartition, OffsetAndMetadata> currentOffsets2 = ImmutableMap.of(
        new TopicPartition("topic1", 1), new OffsetAndMetadata(50, null),
        new TopicPartition("topic1", 2), new OffsetAndMetadata(0, null),
        new TopicPartition("topic3", 1), new OffsetAndMetadata(100, null),
        new TopicPartition("topic99", 1), new OffsetAndMetadata(99, null)
    );

    Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> endOffsets2 = ImmutableMap.of(
        new TopicPartition("topic1", 1), listOffsetResult(75L),
        new TopicPartition("topic1", 2), listOffsetResult(25L),
        new TopicPartition("topic3", 1), listOffsetResult(99L)
    );

    ConsumerGroupOffsets offsets2 = dao.getConsumerGroupOffsets(cgDesc2, currentOffsets2, beginningOffsets, endOffsets2);
    assertEquals("cg2", offsets2.getConsumerGroupId());
    assertEquals(150, offsets2.getSumCurrentOffset());
    assertEquals(199, offsets2.getSumEndOffset());
    assertEquals(49, offsets2.getTotalLag());
    assertEquals(0, offsets2.getNumConsumers());
    assertEquals(2, offsets2.consumerGroupOffsets.size());
  }

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
