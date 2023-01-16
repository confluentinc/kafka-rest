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

package io.confluent.kafkarest.resources.v3;

import static io.confluent.kafkarest.common.CompletableFutures.failedFuture;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.junit.Assert.assertEquals;

import io.confluent.kafkarest.controllers.PartitionManager;
import io.confluent.kafkarest.entities.Partition;
import io.confluent.kafkarest.entities.PartitionReplica;
import io.confluent.kafkarest.entities.v3.GetPartitionResponse;
import io.confluent.kafkarest.entities.v3.ListPartitionsResponse;
import io.confluent.kafkarest.entities.v3.PartitionData;
import io.confluent.kafkarest.entities.v3.PartitionDataList;
import io.confluent.kafkarest.entities.v3.Resource;
import io.confluent.kafkarest.entities.v3.ResourceCollection;
import io.confluent.kafkarest.response.CrnFactoryImpl;
import io.confluent.kafkarest.response.FakeAsyncResponse;
import io.confluent.kafkarest.response.FakeUrlFactory;
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import javax.ws.rs.NotFoundException;
import org.easymock.EasyMockRule;
import org.easymock.Mock;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class PartitionsResourceTest {

  private static final String CLUSTER_ID = "cluster-1";
  private static final String TOPIC_NAME = "topic-1";

  private static final Partition PARTITION_1 =
      Partition.create(
          CLUSTER_ID,
          TOPIC_NAME,
          /* partitionId= */ 0,
          Arrays.asList(
              PartitionReplica.create(
                  CLUSTER_ID,
                  TOPIC_NAME,
                  /* partitionId= */ 0,
                  /* brokerId= */ 1,
                  /* isLeader= */ true,
                  /* isInSync= */ false),
              PartitionReplica.create(
                  CLUSTER_ID,
                  TOPIC_NAME,
                  /* partitionId= */ 0,
                  /* brokerId= */ 2,
                  /* isLeader= */ false,
                  /* isInSync= */ true),
              PartitionReplica.create(
                  CLUSTER_ID,
                  TOPIC_NAME,
                  /* partitionId= */ 0,
                  /* brokerId= */ 3,
                  /* isLeader= */ false,
                  /* isInSync= */ false)));
  private static final Partition PARTITION_2 =
      Partition.create(
          CLUSTER_ID,
          TOPIC_NAME,
          /* partitionId= */ 1,
          Arrays.asList(
              PartitionReplica.create(
                  CLUSTER_ID,
                  TOPIC_NAME,
                  /* partitionId= */ 1,
                  /* brokerId= */ 2,
                  /* isLeader= */ true,
                  /* isInSync= */ false),
              PartitionReplica.create(
                  CLUSTER_ID,
                  TOPIC_NAME,
                  /* partitionId= */ 1,
                  /* brokerId= */ 3,
                  /* isLeader= */ false,
                  /* isInSync= */ true),
              PartitionReplica.create(
                  CLUSTER_ID,
                  TOPIC_NAME,
                  /* partitionId= */ 1,
                  /* brokerId= */ 1,
                  /* isLeader= */ false,
                  /* isInSync= */ false)));
  private static final Partition PARTITION_3 =
      Partition.create(
          CLUSTER_ID,
          TOPIC_NAME,
          /* partitionId= */ 2,
          Arrays.asList(
              PartitionReplica.create(
                  CLUSTER_ID,
                  TOPIC_NAME,
                  /* partitionId= */ 2,
                  /* brokerId= */ 3,
                  /* isLeader= */ true,
                  /* isInSync= */ false),
              PartitionReplica.create(
                  CLUSTER_ID,
                  TOPIC_NAME,
                  /* partitionId= */ 2,
                  /* brokerId= */ 1,
                  /* isLeader= */ false,
                  /* isInSync= */ true),
              PartitionReplica.create(
                  CLUSTER_ID,
                  TOPIC_NAME,
                  /* partitionId= */ 2,
                  /* brokerId= */ 2,
                  /* isLeader= */ false,
                  /* isInSync= */ false)));

  @Rule
  public final EasyMockRule mocks = new EasyMockRule(this);

  @Mock
  private PartitionManager partitionManager;

  private PartitionsResource partitionsResource;

  @Before
  public void setUp() {
    partitionsResource =
        new PartitionsResource(
            () -> partitionManager,
            new CrnFactoryImpl(/* crnAuthorityConfig= */ ""),
            new FakeUrlFactory());
  }

  @Test
  public void listPartitions_existingTopic_returnsPartitions() {
    expect(partitionManager.listPartitions(CLUSTER_ID, TOPIC_NAME))
        .andReturn(
            CompletableFuture.completedFuture(
                Arrays.asList(PARTITION_1, PARTITION_2, PARTITION_3)));
    replay(partitionManager);

    FakeAsyncResponse response = new FakeAsyncResponse();
    partitionsResource.listPartitions(response, CLUSTER_ID, TOPIC_NAME);

    ListPartitionsResponse expected =
        ListPartitionsResponse.create(
            PartitionDataList.builder()
                .setMetadata(
                    ResourceCollection.Metadata.builder()
                        .setSelf("/v3/clusters/cluster-1/topics/topic-1/partitions")
                        .build())
                .setData(
                    Arrays.asList(
                        PartitionData.builder()
                            .setMetadata(
                                Resource.Metadata.builder()
                                    .setSelf("/v3/clusters/cluster-1/topics/topic-1/partitions/0")
                                    .setResourceName(
                                        "crn:///kafka=cluster-1/topic=topic-1/partition=0")
                                    .build())
                            .setClusterId(CLUSTER_ID)
                            .setTopicName(TOPIC_NAME)
                            .setPartitionId(PARTITION_1.getPartitionId())
                            .setLeader(
                                Resource.Relationship.create(
                                    "/v3/clusters/cluster-1/topics/topic-1/partitions/0" +
                                        "/replicas/1"))
                            .setReplicas(
                                Resource.Relationship.create(
                                    "/v3/clusters/cluster-1/topics/topic-1/partitions/0/replicas"))
                            .setReassignment(
                                Resource.Relationship.create(
                                    "/v3/clusters/cluster-1/topics/topic-1/partitions/0"
                                        + "/reassignment"))
                            .build(),
                        PartitionData.builder()
                            .setMetadata(
                                Resource.Metadata.builder()
                                    .setSelf("/v3/clusters/cluster-1/topics/topic-1/partitions/1")
                                    .setResourceName(
                                        "crn:///kafka=cluster-1/topic=topic-1/partition=1")
                                    .build())
                            .setClusterId(CLUSTER_ID)
                            .setTopicName(TOPIC_NAME)
                            .setPartitionId(PARTITION_2.getPartitionId())
                            .setLeader(
                                Resource.Relationship.create(
                                    "/v3/clusters/cluster-1/topics/topic-1/partitions/1" +
                                        "/replicas/2"))
                            .setReplicas(
                                Resource.Relationship.create(
                                    "/v3/clusters/cluster-1/topics/topic-1/partitions/1/replicas"))
                            .setReassignment(
                                Resource.Relationship.create(
                                    "/v3/clusters/cluster-1/topics/topic-1/partitions/1"
                                        + "/reassignment"))
                            .build(),
                        PartitionData.builder()
                            .setMetadata(
                                Resource.Metadata.builder()
                                    .setSelf("/v3/clusters/cluster-1/topics/topic-1/partitions/2")
                                    .setResourceName(
                                        "crn:///kafka=cluster-1/topic=topic-1/partition=2")
                                    .build())
                            .setClusterId(CLUSTER_ID)
                            .setTopicName(TOPIC_NAME)
                            .setPartitionId(PARTITION_3.getPartitionId())
                            .setLeader(
                                Resource.Relationship.create(
                                    "/v3/clusters/cluster-1/topics/topic-1/partitions/2" +
                                        "/replicas/3"))
                            .setReplicas(
                                Resource.Relationship.create(
                                    "/v3/clusters/cluster-1/topics/topic-1/partitions/2/replicas"))
                            .setReassignment(
                                Resource.Relationship.create(
                                    "/v3/clusters/cluster-1/topics/topic-1/partitions/2/reassignment"))
                            .build()))
                .build());

    assertEquals(expected, response.getValue());
  }

  @Test
  public void listPartitions_nonExistingTopicOrCluster_throwsNotFound() {
    expect(partitionManager.listPartitions(CLUSTER_ID, TOPIC_NAME))
        .andReturn(failedFuture(new NotFoundException()));
    replay(partitionManager);

    FakeAsyncResponse response = new FakeAsyncResponse();
    partitionsResource.listPartitions(response, CLUSTER_ID, TOPIC_NAME);

    assertEquals(NotFoundException.class, response.getException().getClass());
  }

  @Test
  public void getPartition_existingPartition_returnsPartition() {
    expect(partitionManager.getPartition(CLUSTER_ID, TOPIC_NAME, PARTITION_1.getPartitionId()))
        .andReturn(CompletableFuture.completedFuture(Optional.of(PARTITION_1)));
    replay(partitionManager);

    FakeAsyncResponse response = new FakeAsyncResponse();
    partitionsResource.getPartition(response, CLUSTER_ID, TOPIC_NAME, PARTITION_1.getPartitionId());

    GetPartitionResponse expected =
        GetPartitionResponse.create(
            PartitionData.builder()
                .setMetadata(
                    Resource.Metadata.builder()
                        .setSelf("/v3/clusters/cluster-1/topics/topic-1/partitions/0")
                        .setResourceName("crn:///kafka=cluster-1/topic=topic-1/partition=0")
                        .build())
                .setClusterId(CLUSTER_ID)
                .setTopicName(TOPIC_NAME)
                .setPartitionId(PARTITION_1.getPartitionId())
                .setLeader(
                    Resource.Relationship.create(
                        "/v3/clusters/cluster-1/topics/topic-1/partitions/0/replicas/1"))
                .setReplicas(
                    Resource.Relationship.create(
                        "/v3/clusters/cluster-1/topics/topic-1/partitions/0/replicas"))
                .setReassignment(
                    Resource.Relationship.create(
                        "/v3/clusters/cluster-1/topics/topic-1/partitions/0/reassignment"))
                .build());

    assertEquals(expected, response.getValue());
  }

  @Test
  public void getPartition_nonExistingPartition_throwsNotFound() {
    expect(partitionManager.getPartition(CLUSTER_ID, TOPIC_NAME, PARTITION_1.getPartitionId()))
        .andReturn(CompletableFuture.completedFuture(Optional.empty()));
    replay(partitionManager);

    FakeAsyncResponse response = new FakeAsyncResponse();
    partitionsResource.getPartition(response, CLUSTER_ID, TOPIC_NAME, PARTITION_1.getPartitionId());

    assertEquals(NotFoundException.class, response.getException().getClass());
  }

  @Test
  public void getPartition_nonExistingTopicOrCluster_throwsNotFound() {
    expect(partitionManager.getPartition(CLUSTER_ID, TOPIC_NAME, PARTITION_1.getPartitionId()))
        .andReturn(failedFuture(new NotFoundException()));
    replay(partitionManager);

    FakeAsyncResponse response = new FakeAsyncResponse();
    partitionsResource.getPartition(response, CLUSTER_ID, TOPIC_NAME, PARTITION_1.getPartitionId());

    assertEquals(NotFoundException.class, response.getException().getClass());
  }
}
