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
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

import io.confluent.kafkarest.entities.Partition;
import io.confluent.kafkarest.entities.PartitionReplica;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
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
public class ReplicaManagerImplTest {

  private static final String CLUSTER_ID = "cluster-1";
  private static final String TOPIC_NAME = "topic-1";
  private static final int PARTITION_ID = 0;

  private static final PartitionReplica REPLICA_1 =
      new PartitionReplica(
          CLUSTER_ID,
          TOPIC_NAME,
          PARTITION_ID,
          /* brokerId= */ 1,
          /* isLeader= */ true,
          /* isInSync= */ false);
  private static final PartitionReplica REPLICA_2 =
      new PartitionReplica(
          CLUSTER_ID,
          TOPIC_NAME,
          PARTITION_ID,
          /* brokerId= */ 2,
          /* isLeader= */ false,
          /* isInSync= */ true);
  private static final PartitionReplica REPLICA_3 =
      new PartitionReplica(
          CLUSTER_ID,
          TOPIC_NAME,
          PARTITION_ID,
          /* brokerId= */ 3,
          /* isLeader= */ false,
          /* isInSync= */ false);

  private static final Partition PARTITION =
      new Partition(
          CLUSTER_ID,
          TOPIC_NAME,
          PARTITION_ID,
          Arrays.asList(REPLICA_1, REPLICA_2, REPLICA_3));

  @Rule
  public final EasyMockRule mocks = new EasyMockRule(this);

  @Mock
  private PartitionManager partitionManager;

  private ReplicaManagerImpl replicaManager;

  @Before
  public void setUp() {
    replicaManager = new ReplicaManagerImpl(partitionManager);
  }

  @Test
  public void listReplicas_existingPartition_returnsReplicas() throws Exception {
    expect(partitionManager.getPartition(CLUSTER_ID, TOPIC_NAME, PARTITION_ID))
        .andReturn(completedFuture(Optional.of(PARTITION)));
    replay(partitionManager);

    List<PartitionReplica> replicas =
        replicaManager.listReplicas(CLUSTER_ID, TOPIC_NAME, PARTITION_ID).get();

    assertEquals(Arrays.asList(REPLICA_1, REPLICA_2, REPLICA_3), replicas);
  }

  @Test
  public void listReplicas_nonExistingPartition_throwsNotFound() throws Exception {
    expect(partitionManager.getPartition(CLUSTER_ID, TOPIC_NAME, PARTITION_ID))
        .andReturn(completedFuture(Optional.empty()));
    replay(partitionManager);

    try {
      replicaManager.listReplicas(CLUSTER_ID, TOPIC_NAME, PARTITION_ID).get();
      fail();
    } catch (ExecutionException e) {
      assertEquals(NotFoundException.class, e.getCause().getClass());
    }
  }

  @Test
  public void listReplicas_nonExistingTopicOrCluster_throwsNotFound() throws Exception {
    expect(partitionManager.getPartition(CLUSTER_ID, TOPIC_NAME, PARTITION_ID))
        .andReturn(failedFuture(new NotFoundException()));
    replay(partitionManager);

    try {
      replicaManager.listReplicas(CLUSTER_ID, TOPIC_NAME, PARTITION_ID).get();
      fail();
    } catch (ExecutionException e) {
      assertEquals(NotFoundException.class, e.getCause().getClass());
    }
  }

  @Test
  public void getReplica_existingReplica_returnsReplica() throws Exception {
    expect(partitionManager.getPartition(CLUSTER_ID, TOPIC_NAME, PARTITION_ID))
        .andReturn(completedFuture(Optional.of(PARTITION)));
    replay(partitionManager);

    Optional<PartitionReplica> replica =
        replicaManager.getReplica(CLUSTER_ID, TOPIC_NAME, PARTITION_ID, REPLICA_1.getBrokerId())
            .get();

    assertEquals(REPLICA_1, replica.get());
  }

  @Test
  public void getReplica_nonExistingReplica_returnEmpty() throws Exception {
    expect(partitionManager.getPartition(CLUSTER_ID, TOPIC_NAME, PARTITION_ID))
        .andReturn(completedFuture(Optional.of(PARTITION)));
    replay(partitionManager);

    Optional<PartitionReplica> replica =
        replicaManager.getReplica(CLUSTER_ID, TOPIC_NAME, PARTITION_ID, 100).get();

    assertFalse(replica.isPresent());
  }

  @Test
  public void getReplica_nonExistingPartition_throwsNotFound() throws Exception {
    expect(partitionManager.getPartition(CLUSTER_ID, TOPIC_NAME, PARTITION_ID))
        .andReturn(completedFuture(Optional.empty()));
    replay(partitionManager);

    try {
      replicaManager.getReplica(CLUSTER_ID, TOPIC_NAME, PARTITION_ID, REPLICA_1.getBrokerId())
          .get();
      fail();
    } catch (ExecutionException e) {
      assertEquals(NotFoundException.class, e.getCause().getClass());
    }
  }

  @Test
  public void getReplica_nonExistingTopicOrCluster_throwsNotFound() throws Exception {
    expect(partitionManager.getPartition(CLUSTER_ID, TOPIC_NAME, PARTITION_ID))
        .andReturn(failedFuture(new NotFoundException()));
    replay(partitionManager);

    try {
      replicaManager.getReplica(CLUSTER_ID, TOPIC_NAME, PARTITION_ID, REPLICA_1.getBrokerId())
          .get();
      fail();
    } catch (ExecutionException e) {
      assertEquals(NotFoundException.class, e.getCause().getClass());
    }
  }
}
