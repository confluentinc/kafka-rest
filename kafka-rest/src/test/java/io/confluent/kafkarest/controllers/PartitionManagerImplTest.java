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

import static io.confluent.kafkarest.CompletableFutures.failedFuture;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

import io.confluent.kafkarest.entities.Partition;
import io.confluent.kafkarest.entities.PartitionReplica;
import io.confluent.kafkarest.entities.Topic;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import javax.ws.rs.NotFoundException;
import org.easymock.EasyMockRule;
import org.easymock.Mock;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class PartitionManagerImplTest {

  private static final String CLUSTER_ID = "cluster-1";
  private static final String TOPIC_NAME = "topic-1";

  private static final Partition PARTITION_1 =
      new Partition(
          CLUSTER_ID,
          TOPIC_NAME,
          /* partitionId= */ 0,
          Arrays.asList(
              new PartitionReplica(
                  CLUSTER_ID,
                  TOPIC_NAME,
                  /* partitionId= */ 0,
                  /* brokerId= */ 1,
                  /* isLeader= */ true,
                  /* isInSync= */ false),
              new PartitionReplica(
                  CLUSTER_ID,
                  TOPIC_NAME,
                  /* partitionId= */ 0,
                  /* brokerId= */ 2,
                  /* isLeader= */ false,
                  /* isInSync= */ true),
              new PartitionReplica(
                  CLUSTER_ID,
                  TOPIC_NAME,
                  /* partitionId= */ 0,
                  /* brokerId= */ 3,
                  /* isLeader= */ false,
                  /* isInSync= */ false)));
  private static final Partition PARTITION_2 =
      new Partition(
          CLUSTER_ID,
          TOPIC_NAME,
          /* partitionId= */ 1,
          Arrays.asList(
              new PartitionReplica(
                  CLUSTER_ID,
                  TOPIC_NAME,
                  /* partitionId= */ 1,
                  /* brokerId= */ 2,
                  /* isLeader= */ true,
                  /* isInSync= */ false),
              new PartitionReplica(
                  CLUSTER_ID,
                  TOPIC_NAME,
                  /* partitionId= */ 1,
                  /* brokerId= */ 3,
                  /* isLeader= */ false,
                  /* isInSync= */ true),
              new PartitionReplica(
                  CLUSTER_ID,
                  TOPIC_NAME,
                  /* partitionId= */ 1,
                  /* brokerId= */ 1,
                  /* isLeader= */ false,
                  /* isInSync= */ false)));
  private static final Partition PARTITION_3 =
      new Partition(
          CLUSTER_ID,
          TOPIC_NAME,
          /* partitionId= */ 2,
          Arrays.asList(
              new PartitionReplica(
                  CLUSTER_ID,
                  TOPIC_NAME,
                  /* partitionId= */ 2,
                  /* brokerId= */ 3,
                  /* isLeader= */ true,
                  /* isInSync= */ false),
              new PartitionReplica(
                  CLUSTER_ID,
                  TOPIC_NAME,
                  /* partitionId= */ 2,
                  /* brokerId= */ 1,
                  /* isLeader= */ false,
                  /* isInSync= */ true),
              new PartitionReplica(
                  CLUSTER_ID,
                  TOPIC_NAME,
                  /* partitionId= */ 2,
                  /* brokerId= */ 2,
                  /* isLeader= */ false,
                  /* isInSync= */ false)));

  private static final Topic TOPIC =
      new Topic(
          CLUSTER_ID,
          TOPIC_NAME,
          Arrays.asList(PARTITION_1, PARTITION_2, PARTITION_3),
          /* replicationFactor= */ (short) 3,
          /* isInternal= */ false);

  @Rule
  public final EasyMockRule mocks = new EasyMockRule(this);

  @Mock
  private TopicManager topicManager;

  private PartitionManagerImpl partitionManager;

  @Before
  public void setUp() {
    partitionManager = new PartitionManagerImpl(topicManager);
  }

  @Test
  public void listPartitions_existingTopic_returnsPartitions() throws Exception {
    expect(topicManager.getTopic(CLUSTER_ID, TOPIC_NAME))
        .andReturn(CompletableFuture.completedFuture(Optional.of(TOPIC)));
    replay(topicManager);

    List<Partition> partitions = partitionManager.listPartitions(CLUSTER_ID, TOPIC_NAME).get();

    assertEquals(Arrays.asList(PARTITION_1, PARTITION_2, PARTITION_3), partitions);
  }

  @Test
  public void listPartitions_nonExistingTopic_throwsNotFound() throws Exception {
    expect(topicManager.getTopic(CLUSTER_ID, TOPIC_NAME))
        .andReturn(CompletableFuture.completedFuture(Optional.empty()));
    replay(topicManager);

    try {
      partitionManager.listPartitions(CLUSTER_ID, TOPIC_NAME).get();
      fail();
    } catch (ExecutionException e) {
      assertEquals(NotFoundException.class, e.getCause().getClass());
    }
  }

  @Test
  public void listPartitions_nonExistingCluster_throwsNotFound() throws Exception {
    expect(topicManager.getTopic(CLUSTER_ID, TOPIC_NAME))
        .andReturn(failedFuture(new NotFoundException()));
    replay(topicManager);

    try {
      partitionManager.listPartitions(CLUSTER_ID, TOPIC_NAME).get();
      fail();
    } catch (ExecutionException e) {
      assertEquals(NotFoundException.class, e.getCause().getClass());
    }
  }

  @Test
  public void getPartition_existingPartition_returnsPartition() throws Exception {
    expect(topicManager.getTopic(CLUSTER_ID, TOPIC_NAME))
        .andReturn(CompletableFuture.completedFuture(Optional.of(TOPIC)));
    replay(topicManager);

    Optional<Partition> partition =
        partitionManager.getPartition(CLUSTER_ID, TOPIC_NAME, PARTITION_1.getPartitionId()).get();

    assertEquals(PARTITION_1, partition.get());
  }

  @Test
  public void getPartition_nonExistingPartition_returnsEmpty() throws Exception {
    expect(topicManager.getTopic(CLUSTER_ID, TOPIC_NAME))
        .andReturn(CompletableFuture.completedFuture(Optional.of(TOPIC)));
    replay(topicManager);

    Optional<Partition> partition =
        partitionManager.getPartition(CLUSTER_ID, TOPIC_NAME, 100).get();

    assertFalse(partition.isPresent());
  }

  @Test
  public void getPartition_nonExistingTopic_throwsNotFound() throws Exception {
    expect(topicManager.getTopic(CLUSTER_ID, TOPIC_NAME))
        .andReturn(CompletableFuture.completedFuture(Optional.empty()));
    replay(topicManager);

    try {
      partitionManager.getPartition(CLUSTER_ID, TOPIC_NAME, PARTITION_1.getPartitionId()).get();
      fail();
    } catch (ExecutionException e) {
      assertEquals(NotFoundException.class, e.getCause().getClass());
    }
  }

  @Test
  public void getPartition_nonExistingCluster_throwsNotFound() throws Exception {
    expect(topicManager.getTopic(CLUSTER_ID, TOPIC_NAME))
        .andReturn(failedFuture(new NotFoundException()));
    replay(topicManager);

    try {
      partitionManager.getPartition(CLUSTER_ID, TOPIC_NAME, PARTITION_1.getPartitionId()).get();
      fail();
    } catch (ExecutionException e) {
      assertEquals(NotFoundException.class, e.getCause().getClass());
    }
  }
}
