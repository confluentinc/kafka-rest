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

import static io.confluent.kafkarest.common.CompletableFutures.failedFuture;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.fail;

import io.confluent.kafkarest.entities.Broker;
import io.confluent.kafkarest.entities.Partition;
import io.confluent.kafkarest.entities.PartitionReplica;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import javax.ws.rs.NotFoundException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DescribeLogDirsResult;
import org.apache.kafka.clients.admin.LogDirDescription;
import org.apache.kafka.clients.admin.ReplicaInfo;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.easymock.EasyMockExtension;
import org.easymock.Mock;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(EasyMockExtension.class)
public class ReplicaManagerImplTest {

  private static final String CLUSTER_ID = "cluster-1";
  private static final String TOPIC_NAME = "topic-1";
  private static final int PARTITION_ID_1 = 0;
  private static final int PARTITION_ID_2 = 1;
  private static final int BROKER_ID_1 = 1;
  private static final int BROKER_ID_2 = 2;
  private static final int BROKER_ID_3 = 3;

  private static final Broker BROKER_1 =
      Broker.create(CLUSTER_ID, BROKER_ID_1, "1.2.3.4", 5, /* rack= */ null);

  private static final PartitionReplica REPLICA_1_1 =
      PartitionReplica.create(
          CLUSTER_ID,
          TOPIC_NAME,
          PARTITION_ID_1,
          BROKER_ID_1,
          /* isLeader= */ true,
          /* isInSync= */ false);
  private static final PartitionReplica REPLICA_1_2 =
      PartitionReplica.create(
          CLUSTER_ID,
          TOPIC_NAME,
          PARTITION_ID_1,
          BROKER_ID_2,
          /* isLeader= */ false,
          /* isInSync= */ true);
  private static final PartitionReplica REPLICA_1_3 =
      PartitionReplica.create(
          CLUSTER_ID,
          TOPIC_NAME,
          PARTITION_ID_1,
          BROKER_ID_3,
          /* isLeader= */ false,
          /* isInSync= */ false);
  private static final PartitionReplica REPLICA_2_1 =
      PartitionReplica.create(
          CLUSTER_ID,
          TOPIC_NAME,
          PARTITION_ID_2,
          BROKER_ID_1,
          /* isLeader= */ false,
          /* isInSync= */ false);
  private static final PartitionReplica REPLICA_2_2 =
      PartitionReplica.create(
          CLUSTER_ID,
          TOPIC_NAME,
          PARTITION_ID_2,
          BROKER_ID_2,
          /* isLeader= */ true,
          /* isInSync= */ true);

  private static final Partition PARTITION_1 =
      Partition.create(
          CLUSTER_ID,
          TOPIC_NAME,
          PARTITION_ID_1,
          Arrays.asList(REPLICA_1_1, REPLICA_1_2, REPLICA_1_3));
  private static final Partition PARTITION_2 =
      Partition.create(
          CLUSTER_ID, TOPIC_NAME, PARTITION_ID_2, Arrays.asList(REPLICA_2_1, REPLICA_2_2));

  @Mock private Admin adminClient;

  @Mock private DescribeLogDirsResult describeLogDirsResult;

  @Mock private BrokerManager brokerManager;

  @Mock private PartitionManager partitionManager;

  private ReplicaManagerImpl replicaManager;

  @BeforeEach
  public void setUp() {
    replicaManager = new ReplicaManagerImpl(adminClient, brokerManager, partitionManager);
  }

  @Test
  public void listReplicas_existingPartition_returnsReplicas() throws Exception {
    expect(partitionManager.getPartition(CLUSTER_ID, TOPIC_NAME, PARTITION_ID_1))
        .andReturn(completedFuture(Optional.of(PARTITION_1)));
    replay(partitionManager);

    List<PartitionReplica> replicas =
        replicaManager.listReplicas(CLUSTER_ID, TOPIC_NAME, PARTITION_ID_1).get();

    assertEquals(Arrays.asList(REPLICA_1_1, REPLICA_1_2, REPLICA_1_3), replicas);
  }

  @Test
  public void listReplicas_nonExistingPartition_throwsNotFound() throws Exception {
    expect(partitionManager.getPartition(CLUSTER_ID, TOPIC_NAME, PARTITION_ID_1))
        .andReturn(completedFuture(Optional.empty()));
    replay(partitionManager);

    try {
      replicaManager.listReplicas(CLUSTER_ID, TOPIC_NAME, PARTITION_ID_1).get();
      fail();
    } catch (ExecutionException e) {
      assertEquals(NotFoundException.class, e.getCause().getClass());
    }
  }

  @Test
  public void listReplicas_nonExistingTopicOrCluster_throwsNotFound() throws Exception {
    expect(partitionManager.getPartition(CLUSTER_ID, TOPIC_NAME, PARTITION_ID_1))
        .andReturn(failedFuture(new NotFoundException()));
    replay(partitionManager);

    try {
      replicaManager.listReplicas(CLUSTER_ID, TOPIC_NAME, PARTITION_ID_1).get();
      fail();
    } catch (ExecutionException e) {
      assertEquals(NotFoundException.class, e.getCause().getClass());
    }
  }

  @Test
  public void getReplica_existingReplica_returnsReplica() throws Exception {
    expect(partitionManager.getPartition(CLUSTER_ID, TOPIC_NAME, PARTITION_ID_1))
        .andReturn(completedFuture(Optional.of(PARTITION_1)));
    replay(partitionManager);

    Optional<PartitionReplica> replica =
        replicaManager
            .getReplica(CLUSTER_ID, TOPIC_NAME, PARTITION_ID_1, REPLICA_1_1.getBrokerId())
            .get();

    assertEquals(REPLICA_1_1, replica.get());
  }

  @Test
  public void getReplica_nonExistingReplica_returnEmpty() throws Exception {
    expect(partitionManager.getPartition(CLUSTER_ID, TOPIC_NAME, PARTITION_ID_1))
        .andReturn(completedFuture(Optional.of(PARTITION_1)));
    replay(partitionManager);

    Optional<PartitionReplica> replica =
        replicaManager.getReplica(CLUSTER_ID, TOPIC_NAME, PARTITION_ID_1, 100).get();

    assertFalse(replica.isPresent());
  }

  @Test
  public void getReplica_nonExistingPartition_throwsNotFound() throws Exception {
    expect(partitionManager.getPartition(CLUSTER_ID, TOPIC_NAME, PARTITION_ID_1))
        .andReturn(completedFuture(Optional.empty()));
    replay(partitionManager);

    try {
      replicaManager
          .getReplica(CLUSTER_ID, TOPIC_NAME, PARTITION_ID_1, REPLICA_1_1.getBrokerId())
          .get();
      fail();
    } catch (ExecutionException e) {
      assertEquals(NotFoundException.class, e.getCause().getClass());
    }
  }

  @Test
  public void getReplica_nonExistingTopicOrCluster_throwsNotFound() throws Exception {
    expect(partitionManager.getPartition(CLUSTER_ID, TOPIC_NAME, PARTITION_ID_1))
        .andReturn(failedFuture(new NotFoundException()));
    replay(partitionManager);

    try {
      replicaManager
          .getReplica(CLUSTER_ID, TOPIC_NAME, PARTITION_ID_1, REPLICA_1_1.getBrokerId())
          .get();
      fail();
    } catch (ExecutionException e) {
      assertEquals(NotFoundException.class, e.getCause().getClass());
    }
  }

  @Test
  public void searchByBrokerId_existingBroker_returnsReplicas() throws Exception {
    HashMap<TopicPartition, ReplicaInfo> partitions = new HashMap<>();
    partitions.put(new TopicPartition(TOPIC_NAME, PARTITION_ID_1), null);
    partitions.put(new TopicPartition(TOPIC_NAME, PARTITION_ID_2), null);
    expect(brokerManager.getBroker(CLUSTER_ID, BROKER_ID_1))
        .andReturn(completedFuture(Optional.of(BROKER_1)));
    expect(adminClient.describeLogDirs(eq(singletonList(BROKER_ID_1)), anyObject()))
        .andReturn(describeLogDirsResult);
    expect(describeLogDirsResult.descriptions())
        .andReturn(
            singletonMap(
                BROKER_ID_1,
                KafkaFuture.completedFuture(
                    singletonMap(null, new LogDirDescription(null, partitions)))));
    expect(partitionManager.getPartitionAllowMissing(CLUSTER_ID, TOPIC_NAME, PARTITION_ID_1))
        .andReturn(completedFuture(Optional.of(PARTITION_1)));
    expect(partitionManager.getPartitionAllowMissing(CLUSTER_ID, TOPIC_NAME, PARTITION_ID_2))
        .andReturn(completedFuture(Optional.of(PARTITION_2)));
    replay(adminClient, describeLogDirsResult, brokerManager, partitionManager);

    List<PartitionReplica> replicas =
        replicaManager.searchReplicasByBrokerId(CLUSTER_ID, BROKER_ID_1).get();

    assertEquals(new HashSet<>(Arrays.asList(REPLICA_1_1, REPLICA_2_1)), new HashSet<>(replicas));
  }

  @Test
  public void searchByBrokerId_existingBroker_returnsReplicasTopicDeletionRace() throws Exception {
    HashMap<TopicPartition, ReplicaInfo> partitions = new HashMap<>();
    partitions.put(new TopicPartition(TOPIC_NAME, PARTITION_ID_1), null);
    partitions.put(new TopicPartition(TOPIC_NAME, PARTITION_ID_2), null);
    expect(brokerManager.getBroker(CLUSTER_ID, BROKER_ID_1))
        .andReturn(completedFuture(Optional.of(BROKER_1)));
    expect(adminClient.describeLogDirs(eq(singletonList(BROKER_ID_1)), anyObject()))
        .andReturn(describeLogDirsResult);
    expect(describeLogDirsResult.descriptions())
        .andReturn(
            singletonMap(
                BROKER_ID_1,
                KafkaFuture.completedFuture(
                    singletonMap(null, new LogDirDescription(null, partitions)))));
    // This is slightly fake but the idea is that the describeLogDirs returns information which
    // subsequently can't be found by the partition manager. In this test, just one partition has
    // evaporated.
    expect(partitionManager.getPartitionAllowMissing(CLUSTER_ID, TOPIC_NAME, PARTITION_ID_1))
        .andReturn(completedFuture(Optional.empty()));
    expect(partitionManager.getPartitionAllowMissing(CLUSTER_ID, TOPIC_NAME, PARTITION_ID_2))
        .andReturn(completedFuture(Optional.of(PARTITION_2)));
    replay(adminClient, describeLogDirsResult, brokerManager, partitionManager);

    List<PartitionReplica> replicas =
        replicaManager.searchReplicasByBrokerId(CLUSTER_ID, BROKER_ID_1).get();

    assertEquals(new HashSet<>(Arrays.asList(REPLICA_2_1)), new HashSet<>(replicas));
  }

  @Test
  public void searchByBrokerId_existingBroker_returnsEmptyList() throws Exception {
    expect(brokerManager.getBroker(CLUSTER_ID, BROKER_ID_1))
        .andReturn(completedFuture(Optional.of(BROKER_1)));
    expect(adminClient.describeLogDirs(eq(singletonList(BROKER_ID_1)), anyObject()))
        .andReturn(describeLogDirsResult);
    expect(describeLogDirsResult.descriptions())
        .andReturn(
            singletonMap(
                BROKER_ID_1,
                KafkaFuture.completedFuture(
                    singletonMap(null, new LogDirDescription(null, Collections.emptyMap())))));
    replay(adminClient, describeLogDirsResult, brokerManager);

    List<PartitionReplica> replicas =
        replicaManager.searchReplicasByBrokerId(CLUSTER_ID, BROKER_ID_1).get();

    assertEquals(new HashSet<>(), new HashSet<>(replicas));
  }

  @Test
  public void searchByBrokerId_nonExistingBroker_throwsNotFound() throws Exception {
    expect(brokerManager.getBroker(CLUSTER_ID, BROKER_ID_1))
        .andReturn(completedFuture(Optional.empty()));
    replay(brokerManager);

    try {
      replicaManager.searchReplicasByBrokerId(CLUSTER_ID, BROKER_ID_1).get();
      fail();
    } catch (ExecutionException e) {
      assertEquals(NotFoundException.class, e.getCause().getClass());
    }
  }
}
