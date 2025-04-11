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
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.fail;

import io.confluent.kafkarest.entities.Broker;
import io.confluent.kafkarest.entities.Cluster;
import io.confluent.kafkarest.entities.Consumer;
import io.confluent.kafkarest.entities.ConsumerGroup;
import io.confluent.kafkarest.entities.ConsumerGroup.State;
import io.confluent.kafkarest.entities.ConsumerGroup.Type;
import io.confluent.kafkarest.entities.Partition;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.ws.rs.NotFoundException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.DescribeConsumerGroupsResult;
import org.apache.kafka.clients.admin.ListConsumerGroupsResult;
import org.apache.kafka.clients.admin.MemberAssignment;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.common.KafkaFuture;
import org.easymock.EasyMockExtension;
import org.easymock.Mock;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(EasyMockExtension.class)
public class ConsumerGroupManagerImplTest {

  private static final String CLUSTER_ID = "cluster-1";

  private static final Broker BROKER_1 =
      Broker.create(
          CLUSTER_ID, /* brokerId= */ 1, /* host= */ "1.2.3.4", /* port= */ 1000, /* rack= */ null);
  private static final Broker BROKER_2 =
      Broker.create(
          CLUSTER_ID, /* brokerId= */ 2, /* host= */ "5.6.7.8", /* port= */ 2000, /* rack= */ null);

  private static final Cluster CLUSTER =
      Cluster.create(CLUSTER_ID, BROKER_1, Arrays.asList(BROKER_1, BROKER_2));

  private static final Consumer[][] CONSUMERS = {
    {
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
                      /* topicName= */ "topic-2",
                      /* partitionId= */ 2,
                      /* replicas= */ emptyList()),
                  Partition.create(
                      CLUSTER_ID,
                      /* topicName= */ "topic-3",
                      /* partitionId= */ 3,
                      /* replicas= */ emptyList())))
          .build(),
      Consumer.builder()
          .setClusterId(CLUSTER_ID)
          .setConsumerGroupId("consumer-group-1")
          .setConsumerId("consumer-2")
          .setClientId("client-2")
          .setInstanceId("instance-2")
          .setHost("21.22.23.24")
          .setAssignedPartitions(
              Arrays.asList(
                  Partition.create(
                      CLUSTER_ID,
                      /* topicName= */ "topic-1",
                      /* partitionId= */ 4,
                      /* replicas= */ emptyList()),
                  Partition.create(
                      CLUSTER_ID,
                      /* topicName= */ "topic-2",
                      /* partitionId= */ 5,
                      /* replicas= */ emptyList()),
                  Partition.create(
                      CLUSTER_ID,
                      /* topicName= */ "topic-3",
                      /* partitionId= */ 6,
                      /* replicas= */ emptyList())))
          .build(),
      Consumer.builder()
          .setClusterId(CLUSTER_ID)
          .setConsumerGroupId("consumer-group-1")
          .setConsumerId("consumer-3")
          .setClientId("client-3")
          .setInstanceId("instance-3")
          .setHost("31.32.33.34")
          .setAssignedPartitions(
              Arrays.asList(
                  Partition.create(
                      CLUSTER_ID,
                      /* topicName= */ "topic-1",
                      /* partitionId= */ 7,
                      /* replicas= */ emptyList()),
                  Partition.create(
                      CLUSTER_ID,
                      /* topicName= */ "topic-2",
                      /* partitionId= */ 8,
                      /* replicas= */ emptyList()),
                  Partition.create(
                      CLUSTER_ID,
                      /* topicName= */ "topic-3",
                      /* partitionId= */ 9,
                      /* replicas= */ emptyList())))
          .build()
    },
    {
      Consumer.builder()
          .setClusterId(CLUSTER_ID)
          .setConsumerGroupId("consumer-group-2")
          .setConsumerId("consumer-4")
          .setClientId("client-4")
          .setInstanceId("instance-4")
          .setHost("41.42.43.44")
          .setAssignedPartitions(
              Arrays.asList(
                  Partition.create(
                      CLUSTER_ID,
                      /* topicName= */ "topic-4",
                      /* partitionId= */ 1,
                      /* replicas= */ emptyList()),
                  Partition.create(
                      CLUSTER_ID,
                      /* topicName= */ "topic-5",
                      /* partitionId= */ 2,
                      /* replicas= */ emptyList()),
                  Partition.create(
                      CLUSTER_ID,
                      /* topicName= */ "topic-6",
                      /* partitionId= */ 3,
                      /* replicas= */ emptyList())))
          .build(),
      Consumer.builder()
          .setClusterId(CLUSTER_ID)
          .setConsumerGroupId("consumer-group-2")
          .setConsumerId("consumer-5")
          .setClientId("client-5")
          .setInstanceId("instance-5")
          .setHost("51.52.53.54")
          .setAssignedPartitions(
              Arrays.asList(
                  Partition.create(
                      CLUSTER_ID,
                      /* topicName= */ "topic-4",
                      /* partitionId= */ 4,
                      /* replicas= */ emptyList()),
                  Partition.create(
                      CLUSTER_ID,
                      /* topicName= */ "topic-5",
                      /* partitionId= */ 5,
                      /* replicas= */ emptyList()),
                  Partition.create(
                      CLUSTER_ID,
                      /* topicName= */ "topic-6",
                      /* partitionId= */ 6,
                      /* replicas= */ emptyList())))
          .build(),
      Consumer.builder()
          .setClusterId(CLUSTER_ID)
          .setConsumerGroupId("consumer-group-2")
          .setConsumerId("consumer-6")
          .setClientId("client-6")
          .setInstanceId("instance-6")
          .setHost("61.62.63.64")
          .setAssignedPartitions(
              Arrays.asList(
                  Partition.create(
                      CLUSTER_ID,
                      /* topicName= */ "topic-7",
                      /* partitionId= */ 7,
                      /* replicas= */ emptyList()),
                  Partition.create(
                      CLUSTER_ID,
                      /* topicName= */ "topic-8",
                      /* partitionId= */ 8,
                      /* replicas= */ emptyList()),
                  Partition.create(
                      CLUSTER_ID,
                      /* topicName= */ "topic-9",
                      /* partitionId= */ 9,
                      /* replicas= */ emptyList())))
          .build()
    },
    {
      Consumer.builder()
          .setClusterId(CLUSTER_ID)
          .setConsumerGroupId("consumer-group-3")
          .setConsumerId("consumer-7")
          .setClientId("client-7")
          .setInstanceId("instance-7")
          .setHost("71.72.73.74")
          .setAssignedPartitions(
              Arrays.asList(
                  Partition.create(
                      CLUSTER_ID,
                      /* topicName= */ "topic-4",
                      /* partitionId= */ 1,
                      /* replicas= */ emptyList()),
                  Partition.create(
                      CLUSTER_ID,
                      /* topicName= */ "topic-5",
                      /* partitionId= */ 2,
                      /* replicas= */ emptyList()),
                  Partition.create(
                      CLUSTER_ID,
                      /* topicName= */ "topic-6",
                      /* partitionId= */ 3,
                      /* replicas= */ emptyList())))
          .build(),
      Consumer.builder()
          .setClusterId(CLUSTER_ID)
          .setConsumerGroupId("consumer-group-3")
          .setConsumerId("consumer-8")
          .setClientId("client-8")
          .setInstanceId("instance-8")
          .setHost("81.82.83.84")
          .setAssignedPartitions(
              Arrays.asList(
                  Partition.create(
                      CLUSTER_ID,
                      /* topicName= */ "topic-4",
                      /* partitionId= */ 4,
                      /* replicas= */ emptyList()),
                  Partition.create(
                      CLUSTER_ID,
                      /* topicName= */ "topic-5",
                      /* partitionId= */ 5,
                      /* replicas= */ emptyList()),
                  Partition.create(
                      CLUSTER_ID,
                      /* topicName= */ "topic-6",
                      /* partitionId= */ 6,
                      /* replicas= */ emptyList())))
          .build(),
      Consumer.builder()
          .setClusterId(CLUSTER_ID)
          .setConsumerGroupId("consumer-group-3")
          .setConsumerId("consumer-9")
          .setClientId("client-9")
          .setInstanceId("instance-9")
          .setHost("91.92.93.94")
          .setAssignedPartitions(
              Arrays.asList(
                  Partition.create(
                      CLUSTER_ID,
                      /* topicName= */ "topic-7",
                      /* partitionId= */ 7,
                      /* replicas= */ emptyList()),
                  Partition.create(
                      CLUSTER_ID,
                      /* topicName= */ "topic-8",
                      /* partitionId= */ 8,
                      /* replicas= */ emptyList()),
                  Partition.create(
                      CLUSTER_ID,
                      /* topicName= */ "topic-9",
                      /* partitionId= */ 9,
                      /* replicas= */ emptyList())))
          .build()
    },
    {
      Consumer.builder()
          .setClusterId(CLUSTER_ID)
          .setConsumerGroupId("consumer-group-4")
          .setConsumerId("consumer-10")
          .setClientId("client-10")
          .setInstanceId("instance-10")
          .setHost("101.102.103.104")
          .setAssignedPartitions(
              Arrays.asList(
                  Partition.create(
                      CLUSTER_ID,
                      /* topicName= */ "topic-4",
                      /* partitionId= */ 1,
                      /* replicas= */ emptyList()),
                  Partition.create(
                      CLUSTER_ID,
                      /* topicName= */ "topic-5",
                      /* partitionId= */ 2,
                      /* replicas= */ emptyList()),
                  Partition.create(
                      CLUSTER_ID,
                      /* topicName= */ "topic-6",
                      /* partitionId= */ 3,
                      /* replicas= */ emptyList())))
          .build(),
      Consumer.builder()
          .setClusterId(CLUSTER_ID)
          .setConsumerGroupId("consumer-group-4")
          .setConsumerId("consumer-11")
          .setClientId("client-11")
          .setInstanceId("instance-11")
          .setHost("111.112.113.114")
          .setAssignedPartitions(
              Arrays.asList(
                  Partition.create(
                      CLUSTER_ID,
                      /* topicName= */ "topic-4",
                      /* partitionId= */ 4,
                      /* replicas= */ emptyList()),
                  Partition.create(
                      CLUSTER_ID,
                      /* topicName= */ "topic-5",
                      /* partitionId= */ 5,
                      /* replicas= */ emptyList()),
                  Partition.create(
                      CLUSTER_ID,
                      /* topicName= */ "topic-6",
                      /* partitionId= */ 6,
                      /* replicas= */ emptyList())))
          .build(),
      Consumer.builder()
          .setClusterId(CLUSTER_ID)
          .setConsumerGroupId("consumer-group-4")
          .setConsumerId("consumer-12")
          .setClientId("client-12")
          .setInstanceId("instance-12")
          .setHost("121.122.123.124")
          .setAssignedPartitions(
              Arrays.asList(
                  Partition.create(
                      CLUSTER_ID,
                      /* topicName= */ "topic-7",
                      /* partitionId= */ 7,
                      /* replicas= */ emptyList()),
                  Partition.create(
                      CLUSTER_ID,
                      /* topicName= */ "topic-8",
                      /* partitionId= */ 8,
                      /* replicas= */ emptyList()),
                  Partition.create(
                      CLUSTER_ID,
                      /* topicName= */ "topic-9",
                      /* partitionId= */ 9,
                      /* replicas= */ emptyList())))
          .build()
    }
  };

  private static final ConsumerGroup[] CONSUMER_GROUPS = {
    ConsumerGroup.builder()
        .setClusterId(CLUSTER_ID)
        .setConsumerGroupId("consumer-group-1")
        .setSimple(true)
        .setPartitionAssignor("org.apache.kafka.clients.consumer.RangeAssignor")
        .setState(State.STABLE)
        .setType(Type.CLASSIC)
        .setMixedConsumerGroup(false)
        .setCoordinator(BROKER_1)
        .setConsumers(Arrays.asList(CONSUMERS[0]))
        .build(),
    ConsumerGroup.builder()
        .setClusterId(CLUSTER_ID)
        .setConsumerGroupId("consumer-group-2")
        .setSimple(false)
        .setPartitionAssignor("org.apache.kafka.clients.consumer.RoundRobinAssignor")
        .setState(State.COMPLETING_REBALANCE)
        .setType(Type.CLASSIC)
        .setMixedConsumerGroup(false)
        .setCoordinator(BROKER_2)
        .setConsumers(Arrays.asList(CONSUMERS[1]))
        .build(),
    ConsumerGroup.builder()
        .setClusterId(CLUSTER_ID)
        .setConsumerGroupId("consumer-group-3")
        .setSimple(false)
        .setPartitionAssignor("org.apache.kafka.clients.consumer.StickyAssignor")
        .setState(State.RECONCILING)
        .setType(Type.CONSUMER)
        .setMixedConsumerGroup(true)
        .setCoordinator(BROKER_2)
        .setConsumers(Arrays.asList(CONSUMERS[2]))
        .build(),
    ConsumerGroup.builder()
        .setClusterId(CLUSTER_ID)
        .setConsumerGroupId("consumer-group-4")
        .setSimple(false)
        .setPartitionAssignor("org.apache.kafka.clients.consumer.CooperativeStickyAssignor")
        .setState(State.STABLE)
        .setType(Type.CONSUMER)
        .setMixedConsumerGroup(false)
        .setCoordinator(BROKER_2)
        .setConsumers(Arrays.asList(CONSUMERS[3]))
        .build()
  };

  @Mock private ClusterManager clusterManager;

  @Mock private Admin adminClient;

  @Mock private ListConsumerGroupsResult listConsumerGroupsResult;

  @Mock private DescribeConsumerGroupsResult describeConsumerGroupsResult;

  private ConsumerGroupListing[] consumerGroupListings;

  private ConsumerGroupDescription[] consumerGroupDescriptions;

  private MemberDescription[][] memberDescriptions;

  private MemberAssignment[][] memberAssignments;

  private ConsumerGroupManagerImpl consumerGroupManager;

  @BeforeEach
  public void setUp() {
    consumerGroupListings =
        IntStream.range(0, CONSUMER_GROUPS.length)
            .mapToObj(i -> createMock(ConsumerGroupListing.class))
            .toArray(ConsumerGroupListing[]::new);

    consumerGroupDescriptions =
        IntStream.range(0, CONSUMER_GROUPS.length)
            .mapToObj(i -> createMock(ConsumerGroupDescription.class))
            .toArray(ConsumerGroupDescription[]::new);

    memberDescriptions =
        Arrays.stream(CONSUMERS)
            .map(
                consumerArray ->
                    Arrays.stream(consumerArray)
                        .map(consumer -> createMock(MemberDescription.class))
                        .toArray(MemberDescription[]::new))
            .toArray(MemberDescription[][]::new);

    memberAssignments =
        Arrays.stream(CONSUMERS)
            .map(
                consumerArray ->
                    Arrays.stream(consumerArray)
                        .map(consumer -> createMock(MemberAssignment.class))
                        .toArray(MemberAssignment[]::new))
            .toArray(MemberAssignment[][]::new);

    consumerGroupManager = new ConsumerGroupManagerImpl(adminClient, clusterManager);
  }

  @Test
  public void listConsumerGroups_returnsConsumerGroups() throws Exception {
    expect(clusterManager.getCluster(CLUSTER_ID)).andReturn(completedFuture(Optional.of(CLUSTER)));
    expect(adminClient.listConsumerGroups()).andReturn(listConsumerGroupsResult);
    expect(listConsumerGroupsResult.all())
        .andReturn(KafkaFuture.completedFuture(Arrays.asList(consumerGroupListings)));
    for (int i = 0; i < CONSUMER_GROUPS.length; i++) {
      expect(consumerGroupListings[i].groupId()).andReturn(CONSUMER_GROUPS[i].getConsumerGroupId());
      replay(consumerGroupListings[i]);
    }
    expect(
            adminClient.describeConsumerGroups(
                Arrays.stream(CONSUMER_GROUPS)
                    .map(ConsumerGroup::getConsumerGroupId)
                    .collect(Collectors.toList())))
        .andReturn(describeConsumerGroupsResult);
    expect(describeConsumerGroupsResult.all())
        .andReturn(
            KafkaFuture.completedFuture(
                IntStream.range(0, CONSUMER_GROUPS.length)
                    .boxed()
                    .collect(
                        Collectors.toMap(
                            i -> CONSUMER_GROUPS[i].getConsumerGroupId(),
                            i -> consumerGroupDescriptions[i]))));
    for (int i = 0; i < CONSUMER_GROUPS.length; i++) {
      expect(consumerGroupDescriptions[i].groupId())
          .andStubReturn(CONSUMER_GROUPS[i].getConsumerGroupId());
      expect(consumerGroupDescriptions[i].isSimpleConsumerGroup())
          .andStubReturn(CONSUMER_GROUPS[i].isSimple());
      expect(consumerGroupDescriptions[i].partitionAssignor())
          .andStubReturn(CONSUMER_GROUPS[i].getPartitionAssignor());
      expect(consumerGroupDescriptions[i].groupState())
          .andStubReturn(CONSUMER_GROUPS[i].getState().toGroupState());
      expect(consumerGroupDescriptions[i].type())
          .andStubReturn(CONSUMER_GROUPS[i].getType().toGroupType());
      expect(consumerGroupDescriptions[i].coordinator())
          .andStubReturn(CONSUMER_GROUPS[i].getCoordinator().toNode());
      expect(consumerGroupDescriptions[i].members())
          .andStubReturn(Arrays.asList(memberDescriptions[i]));
      replay(consumerGroupDescriptions[i]);
    }
    for (int i = 0; i < CONSUMER_GROUPS.length; i++) {
      for (int j = 0; j < CONSUMER_GROUPS[i].getConsumers().size(); j++) {
        expect(memberDescriptions[i][j].consumerId())
            .andStubReturn(CONSUMERS[i][j].getConsumerId());
        expect(memberDescriptions[i][j].groupInstanceId())
            .andStubReturn(CONSUMERS[i][j].getInstanceId());
        expect(memberDescriptions[i][j].clientId()).andStubReturn(CONSUMERS[i][j].getClientId());
        expect(memberDescriptions[i][j].host()).andStubReturn(CONSUMERS[i][j].getHost());
        expect(memberDescriptions[i][j].upgraded()).andStubReturn(Optional.of(3 * i + j > 6));
        expect(memberDescriptions[i][j].assignment()).andStubReturn(memberAssignments[i][j]);
        expect(memberAssignments[i][j].topicPartitions())
            .andStubReturn(
                CONSUMERS[i][j].getAssignedPartitions().stream()
                    .map(Partition::toTopicPartition)
                    .collect(Collectors.toSet()));
        replay(memberDescriptions[i][j], memberAssignments[i][j]);
      }
    }
    replay(clusterManager, adminClient, listConsumerGroupsResult, describeConsumerGroupsResult);

    List<ConsumerGroup> consumerGroups = consumerGroupManager.listConsumerGroups(CLUSTER_ID).get();

    assertEquals(new HashSet<>(Arrays.asList(CONSUMER_GROUPS)), new HashSet<>(consumerGroups));
  }

  @Test
  public void listConsumerGroups_nonExistentCluster_throwsNotFound() throws Exception {
    expect(clusterManager.getCluster(CLUSTER_ID)).andReturn(completedFuture(Optional.empty()));
    replay(clusterManager);

    try {
      consumerGroupManager.listConsumerGroups(CLUSTER_ID).get();
      fail();
    } catch (ExecutionException e) {
      assertEquals(NotFoundException.class, e.getCause().getClass());
    }
  }

  @Test
  public void getConsumerGroup_returnsConsumerGroup() throws Exception {
    expect(clusterManager.getCluster(CLUSTER_ID)).andReturn(completedFuture(Optional.of(CLUSTER)));
    expect(
            adminClient.describeConsumerGroups(
                singletonList(CONSUMER_GROUPS[0].getConsumerGroupId())))
        .andReturn(describeConsumerGroupsResult);
    expect(describeConsumerGroupsResult.all())
        .andReturn(
            KafkaFuture.completedFuture(
                singletonMap(
                    CONSUMER_GROUPS[0].getConsumerGroupId(), consumerGroupDescriptions[0])));
    expect(consumerGroupDescriptions[0].groupId())
        .andStubReturn(CONSUMER_GROUPS[0].getConsumerGroupId());
    expect(consumerGroupDescriptions[0].isSimpleConsumerGroup())
        .andStubReturn(CONSUMER_GROUPS[0].isSimple());
    expect(consumerGroupDescriptions[0].partitionAssignor())
        .andStubReturn(CONSUMER_GROUPS[0].getPartitionAssignor());
    expect(consumerGroupDescriptions[0].groupState())
        .andStubReturn(CONSUMER_GROUPS[0].getState().toGroupState());
    expect(consumerGroupDescriptions[0].type())
        .andStubReturn(CONSUMER_GROUPS[0].getType().toGroupType());
    expect(consumerGroupDescriptions[0].coordinator())
        .andStubReturn(CONSUMER_GROUPS[0].getCoordinator().toNode());
    expect(consumerGroupDescriptions[0].members())
        .andStubReturn(Arrays.asList(memberDescriptions[0]));
    for (int j = 0; j < CONSUMER_GROUPS[0].getConsumers().size(); j++) {
      expect(memberDescriptions[0][j].consumerId()).andStubReturn(CONSUMERS[0][j].getConsumerId());
      expect(memberDescriptions[0][j].groupInstanceId())
          .andStubReturn(CONSUMERS[0][j].getInstanceId());
      expect(memberDescriptions[0][j].clientId()).andStubReturn(CONSUMERS[0][j].getClientId());
      expect(memberDescriptions[0][j].host()).andStubReturn(CONSUMERS[0][j].getHost());
      expect(memberDescriptions[0][j].assignment()).andStubReturn(memberAssignments[0][j]);
      expect(memberAssignments[0][j].topicPartitions())
          .andStubReturn(
              CONSUMERS[0][j].getAssignedPartitions().stream()
                  .map(Partition::toTopicPartition)
                  .collect(Collectors.toSet()));
      replay(memberDescriptions[0][j], memberAssignments[0][j]);
    }
    replay(
        clusterManager,
        adminClient,
        listConsumerGroupsResult,
        describeConsumerGroupsResult,
        consumerGroupDescriptions[0]);

    ConsumerGroup consumerGroup =
        consumerGroupManager
            .getConsumerGroup(CLUSTER_ID, CONSUMER_GROUPS[0].getConsumerGroupId())
            .get()
            .get();

    assertEquals(CONSUMER_GROUPS[0], consumerGroup);
  }

  @Test
  public void getConsumerGroup_nonExistingCluster_throwsNotFound() throws Exception {
    expect(clusterManager.getCluster(CLUSTER_ID)).andReturn(completedFuture(Optional.empty()));
    replay(clusterManager);

    try {
      consumerGroupManager
          .getConsumerGroup(CLUSTER_ID, CONSUMER_GROUPS[0].getConsumerGroupId())
          .get();
      fail();
    } catch (ExecutionException e) {
      assertEquals(NotFoundException.class, e.getCause().getClass());
    }
  }

  @Test
  public void getConsumerGroup_nonExistingConsumerGroup_returnsEmpty() throws Exception {
    expect(clusterManager.getCluster(CLUSTER_ID)).andReturn(completedFuture(Optional.of(CLUSTER)));
    expect(
            adminClient.describeConsumerGroups(
                singletonList(CONSUMER_GROUPS[0].getConsumerGroupId())))
        .andReturn(describeConsumerGroupsResult);
    expect(describeConsumerGroupsResult.all()).andReturn(KafkaFuture.completedFuture(emptyMap()));
    replay(clusterManager, adminClient, listConsumerGroupsResult, describeConsumerGroupsResult);

    Optional<ConsumerGroup> consumerGroup =
        consumerGroupManager
            .getConsumerGroup(CLUSTER_ID, CONSUMER_GROUPS[0].getConsumerGroupId())
            .get();

    assertFalse(consumerGroup.isPresent());
  }
}
