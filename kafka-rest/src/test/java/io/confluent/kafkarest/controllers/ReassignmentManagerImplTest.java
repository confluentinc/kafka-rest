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

import static io.confluent.kafkarest.common.KafkaFutures.failedFuture;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import io.confluent.kafkarest.entities.Broker;
import io.confluent.kafkarest.entities.Cluster;
import io.confluent.kafkarest.entities.Reassignment;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import javax.ws.rs.NotFoundException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ListPartitionReassignmentsResult;
import org.apache.kafka.clients.admin.PartitionReassignment;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.easymock.EasyMockRule;
import org.easymock.Mock;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ReassignmentManagerImplTest {

  private static final String CLUSTER_ID = "cluster-1";

  private static final Node NODE_1 = new Node(1, "broker-1", 9091);
  private static final Node NODE_2 = new Node(2, "broker-2", 9092);
  private static final Node NODE_3 = new Node(3, "broker-3", 9093);

  private static final Broker BROKER_1 = Broker.fromNode(CLUSTER_ID, NODE_1);
  private static final Broker BROKER_2 = Broker.fromNode(CLUSTER_ID, NODE_2);
  private static final Broker BROKER_3 = Broker.fromNode(CLUSTER_ID, NODE_3);

  private static final String TOPIC_1 = "topic-1";
  private static final int PARTITION_ID_1 = 1;
  private static final int PARTITION_ID_2 = 2;
  private static final int PARTITION_ID_3 = 3;


  private static final Cluster CLUSTER =
      Cluster.create(CLUSTER_ID, BROKER_1, Arrays.asList(BROKER_1, BROKER_2, BROKER_3));

  private static final List<Integer> REPLICAS_1 = Arrays.asList(1, 2, 3, 4, 5);
  private static final List<Integer> REPLICAS_2 = Arrays.asList(1, 2, 3, 4);
  private static final List<Integer> REPLICAS_3 = Arrays.asList(4, 5, 6);

  private static final List<Integer> ADDING_REPLICAS_1 = Arrays.asList(1, 2, 3);
  private static final List<Integer> ADDING_REPLICAS_2 = Arrays.asList(1, 2, 3);
  private static final List<Integer> ADDING_REPLICAS_3 = Arrays.asList(5, 6);

  private static final List<Integer> REMOVING_REPLICAS_1 = Arrays.asList(4, 5);
  private static final List<Integer> REMOVING_REPLICAS_2 = Arrays.asList(4);
  private static final List<Integer> REMOVING_REPLICAS_3 = Arrays.asList(4);


  private static final TopicPartition TOPIC_PARTITION_1 = new TopicPartition(TOPIC_1,
      PARTITION_ID_1);
  private static final TopicPartition TOPIC_PARTITION_2 = new TopicPartition(TOPIC_1,
      PARTITION_ID_2);
  private static final TopicPartition TOPIC_PARTITION_3 = new TopicPartition(TOPIC_1,
      PARTITION_ID_3);

  private static final PartitionReassignment PARTITION_REASSIGNMENT_1 =
      new PartitionReassignment(REPLICAS_1, ADDING_REPLICAS_1, REMOVING_REPLICAS_1);
  private static final PartitionReassignment PARTITION_REASSIGNMENT_2 =
      new PartitionReassignment(REPLICAS_2, ADDING_REPLICAS_2, REMOVING_REPLICAS_2);
  private static final PartitionReassignment PARTITION_REASSIGNMENT_3 =
      new PartitionReassignment(REPLICAS_3, ADDING_REPLICAS_3, REMOVING_REPLICAS_3);

  private static final Map<TopicPartition, PartitionReassignment> REASSIGNMENT_MAP =
      new HashMap<>();

  private static final Reassignment REASSIGNMENT_1 = Reassignment.create(CLUSTER_ID, TOPIC_1,
      PARTITION_ID_1, ADDING_REPLICAS_1, REMOVING_REPLICAS_1);
  private static final Reassignment REASSIGNMENT_2 = Reassignment.create(CLUSTER_ID, TOPIC_1,
      PARTITION_ID_2, ADDING_REPLICAS_2, REMOVING_REPLICAS_2);
  private static final Reassignment REASSIGNMENT_3 = Reassignment.create(CLUSTER_ID, TOPIC_1,
      PARTITION_ID_3, ADDING_REPLICAS_3, REMOVING_REPLICAS_3);

  @Rule
  public final EasyMockRule mocks = new EasyMockRule(this);

  @Mock
  private Admin adminClient;

  @Mock
  private ListPartitionReassignmentsResult listPartitionReassignmentsResult;

  @Mock
  private ClusterManager clusterManager;

  private ReassignmentManagerImpl reassignmentManager;

  @Before
  public void setUp() {
    reassignmentManager = new ReassignmentManagerImpl(adminClient, clusterManager);
    REASSIGNMENT_MAP.put(TOPIC_PARTITION_1, PARTITION_REASSIGNMENT_1);
    REASSIGNMENT_MAP.put(TOPIC_PARTITION_2, PARTITION_REASSIGNMENT_2);
    REASSIGNMENT_MAP.put(TOPIC_PARTITION_3, PARTITION_REASSIGNMENT_3);
  }

  @Test
  public void listAllReassignments_existingCluster_returnsReassignments() throws Exception {
    expect(clusterManager.getCluster(CLUSTER_ID)).andReturn(completedFuture(Optional.of(CLUSTER)));
    expect(adminClient.listPartitionReassignments()).andReturn(listPartitionReassignmentsResult);
    expect(listPartitionReassignmentsResult.reassignments())
        .andReturn(KafkaFuture.completedFuture(REASSIGNMENT_MAP));
    replay(clusterManager, adminClient, listPartitionReassignmentsResult);

    List<Reassignment> reassignments = reassignmentManager.listReassignments(CLUSTER_ID)
        .get();

    assertEquals(Arrays.asList(REASSIGNMENT_1, REASSIGNMENT_2, REASSIGNMENT_3), reassignments);
  }

  @Test
  public void listAllReassignments_timeoutException_throwsTimeoutException() throws Exception {
    expect(clusterManager.getCluster(CLUSTER_ID)).andReturn(completedFuture(Optional.of(CLUSTER)));
    expect(adminClient.listPartitionReassignments()).andReturn(listPartitionReassignmentsResult);
    expect(listPartitionReassignmentsResult.reassignments())
        .andReturn(failedFuture(new TimeoutException()));
    replay(clusterManager, adminClient, listPartitionReassignmentsResult);

    try {
      reassignmentManager.listReassignments(CLUSTER_ID).get();
      fail();
    } catch (ExecutionException e) {
      assertEquals(TimeoutException.class, e.getCause().getClass());
    }
  }

  @Test
  public void listAllReassignments_nonExistingCluster_throwsNotFound() throws Exception {
    expect(clusterManager.getCluster(CLUSTER_ID)).andReturn(completedFuture(Optional.empty()));
    replay(clusterManager);

    try {
      reassignmentManager.listReassignments(CLUSTER_ID).get();
      fail();
    } catch (ExecutionException e) {
      assertEquals(NotFoundException.class, e.getCause().getClass());
    }
  }

  @Test
  public void searchReassignmentsByTopic_existingCluster_returnsReassignments() throws Exception {
    expect(clusterManager.getCluster(CLUSTER_ID)).andReturn(completedFuture(Optional.of(CLUSTER)));
    expect(adminClient.listPartitionReassignments()).andReturn(listPartitionReassignmentsResult);
    expect(listPartitionReassignmentsResult.reassignments())
        .andReturn(KafkaFuture.completedFuture(REASSIGNMENT_MAP));
    replay(clusterManager, adminClient, listPartitionReassignmentsResult);

    List<Reassignment> reassignments =
        reassignmentManager.searchReassignmentsByTopicName(CLUSTER_ID, TOPIC_1).get();

    assertEquals(Arrays.asList(REASSIGNMENT_1, REASSIGNMENT_2, REASSIGNMENT_3), reassignments);
  }

  @Test
  public void searchReassignmentsByTopic_nonExistingCluster_returnsNotFound() throws Exception {
    expect(clusterManager.getCluster(CLUSTER_ID)).andReturn(completedFuture(Optional.empty()));
    replay(clusterManager);

    try {
      reassignmentManager.searchReassignmentsByTopicName(CLUSTER_ID, TOPIC_1).get();
      fail();
    } catch (ExecutionException e) {
      assertEquals(NotFoundException.class, e.getCause().getClass());
    }
  }

  @Test
  public void searchReassignmentsByTopic_nonExistingTopic_returnsEmpty() throws Exception {
    expect(clusterManager.getCluster(CLUSTER_ID)).andReturn(completedFuture(Optional.of(CLUSTER)));
    expect(adminClient.listPartitionReassignments()).andReturn(listPartitionReassignmentsResult);
    expect(listPartitionReassignmentsResult.reassignments())
        .andReturn(KafkaFuture.completedFuture(REASSIGNMENT_MAP));
    replay(clusterManager, adminClient, listPartitionReassignmentsResult);

    List<Reassignment> reassignments =
        reassignmentManager.searchReassignmentsByTopicName(CLUSTER_ID, "topic-2").get();

    assertTrue(reassignments.isEmpty());
  }

  @Test
  public void getReassignment_existingClusterTopicPartition_returnsReassignment()
      throws Exception {
    expect(clusterManager.getCluster(CLUSTER_ID)).andReturn(completedFuture(Optional.of(CLUSTER)));
    expect(adminClient.listPartitionReassignments()).andReturn(listPartitionReassignmentsResult);
    expect(listPartitionReassignmentsResult.reassignments())
        .andReturn(KafkaFuture.completedFuture(REASSIGNMENT_MAP));
    replay(clusterManager, adminClient, listPartitionReassignmentsResult);
    Optional<Reassignment> reassignment = reassignmentManager.getReassignment(CLUSTER_ID, TOPIC_1
        , PARTITION_ID_1).get();

    assertEquals(REASSIGNMENT_1, reassignment.get());
  }

  @Test
  public void getReassignment_nonExistingCluster_returnsNotFound() throws Exception {
    expect(clusterManager.getCluster(CLUSTER_ID)).andReturn(completedFuture(Optional.empty()));
    replay(clusterManager);

    try {
      reassignmentManager.getReassignment(CLUSTER_ID, TOPIC_1, PARTITION_ID_1).get();
      fail();
    } catch (ExecutionException e) {
      assertEquals(NotFoundException.class, e.getCause().getClass());
    }
  }

  @Test
  public void getReassignment_nonExistingTopic_returnsEmpty() throws Exception {
    expect(clusterManager.getCluster(CLUSTER_ID)).andReturn(completedFuture(Optional.of(CLUSTER)));
    expect(adminClient.listPartitionReassignments()).andReturn(listPartitionReassignmentsResult);
    expect(listPartitionReassignmentsResult.reassignments())
        .andReturn(KafkaFuture.completedFuture(REASSIGNMENT_MAP));
    replay(clusterManager, adminClient, listPartitionReassignmentsResult);
    Optional<Reassignment> reassignment = reassignmentManager.getReassignment(CLUSTER_ID, "foobar",
        3).get();

    assertFalse(reassignment.isPresent());
  }

  @Test
  public void getReassignment_nonExistingPartition_returnsEmpty() throws Exception {
    expect(clusterManager.getCluster(CLUSTER_ID)).andReturn(completedFuture(Optional.of(CLUSTER)));
    expect(adminClient.listPartitionReassignments()).andReturn(listPartitionReassignmentsResult);
    expect(listPartitionReassignmentsResult.reassignments())
        .andReturn(KafkaFuture.completedFuture(REASSIGNMENT_MAP));
    replay(clusterManager, adminClient, listPartitionReassignmentsResult);

    Optional<Reassignment> reassignment = reassignmentManager.getReassignment(CLUSTER_ID, TOPIC_1
        , 4).get();

    assertFalse(reassignment.isPresent());
  }
}
