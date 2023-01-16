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
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.junit.Assert.assertEquals;

import io.confluent.kafkarest.controllers.ReplicaManager;
import io.confluent.kafkarest.entities.PartitionReplica;
import io.confluent.kafkarest.entities.v3.GetReplicaResponse;
import io.confluent.kafkarest.entities.v3.ListReplicasResponse;
import io.confluent.kafkarest.entities.v3.ReplicaData;
import io.confluent.kafkarest.entities.v3.ReplicaDataList;
import io.confluent.kafkarest.entities.v3.Resource;
import io.confluent.kafkarest.entities.v3.ResourceCollection;
import io.confluent.kafkarest.response.CrnFactoryImpl;
import io.confluent.kafkarest.response.FakeAsyncResponse;
import io.confluent.kafkarest.response.FakeUrlFactory;
import java.util.Arrays;
import java.util.Optional;
import javax.ws.rs.NotFoundException;
import org.easymock.EasyMockRule;
import org.easymock.Mock;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ReplicasResourceTest {

  private static final String CLUSTER_ID = "cluster-1";
  private static final String TOPIC_NAME = "topic-1";
  private static final int PARTITION_ID = 0;

  private static final PartitionReplica REPLICA_1 =
      PartitionReplica.create(
          CLUSTER_ID,
          TOPIC_NAME,
          PARTITION_ID,
          /* brokerId= */ 1,
          /* isLeader= */ true,
          /* isInSync= */ true);
  private static final PartitionReplica REPLICA_2 =
      PartitionReplica.create(
          CLUSTER_ID,
          TOPIC_NAME,
          PARTITION_ID,
          /* brokerId= */ 2,
          /* isLeader= */ false,
          /* isInSync= */ true);
  private static final PartitionReplica REPLICA_3 =
      PartitionReplica.create(
          CLUSTER_ID,
          TOPIC_NAME,
          PARTITION_ID,
          /* brokerId= */ 3,
          /* isLeader= */ false,
          /* isInSync= */ false);

  @Rule
  public final EasyMockRule mocks = new EasyMockRule(this);

  @Mock
  private ReplicaManager replicaManager;

  private ReplicasResource replicasResource;

  @Before
  public void setUp() {
    replicasResource =
        new ReplicasResource(
            () -> replicaManager, new CrnFactoryImpl(/* crnAuthorityConfig= */ ""),
            new FakeUrlFactory());
  }

  @Test
  public void listReplicas_existingPartition_returnsReplicas() {
    expect(replicaManager.listReplicas(CLUSTER_ID, TOPIC_NAME, PARTITION_ID))
        .andReturn(completedFuture(Arrays.asList(REPLICA_1, REPLICA_2, REPLICA_3)));
    replay(replicaManager);

    FakeAsyncResponse response = new FakeAsyncResponse();
    replicasResource.listReplicas(response, CLUSTER_ID, TOPIC_NAME, PARTITION_ID);

    ListReplicasResponse expected =
        ListReplicasResponse.create(
            ReplicaDataList.builder()
                .setMetadata(
                    ResourceCollection.Metadata.builder()
                        .setSelf("/v3/clusters/cluster-1/topics/topic-1/partitions/0/replicas")
                        .build())
                .setData(
                    Arrays.asList(
                        ReplicaData.builder()
                            .setMetadata(
                                Resource.Metadata.builder()
                                    .setSelf(
                                        "/v3/clusters/cluster-1/topics/topic-1/partitions/0"
                                            + "/replicas/1")
                                    .setResourceName(
                                        "crn:///kafka=cluster-1/topic=topic-1/partition=0"
                                            + "/replica=1")
                                    .build())
                            .setClusterId(CLUSTER_ID)
                            .setTopicName(TOPIC_NAME)
                            .setPartitionId(PARTITION_ID)
                            .setBrokerId(REPLICA_1.getBrokerId())
                            .setLeader(true)
                            .setInSync(true)
                            .setBroker(
                                Resource.Relationship.create("/v3/clusters/cluster-1/brokers/1"))
                            .build(),
                        ReplicaData.builder()
                            .setMetadata(
                                Resource.Metadata.builder()
                                    .setSelf(
                                        "/v3/clusters/cluster-1/topics/topic-1/partitions/0"
                                            + "/replicas/2")
                                    .setResourceName(
                                        "crn:///kafka=cluster-1/topic=topic-1/partition=0"
                                            + "/replica=2")
                                    .build())
                            .setClusterId(CLUSTER_ID)
                            .setTopicName(TOPIC_NAME)
                            .setPartitionId(PARTITION_ID)
                            .setBrokerId(REPLICA_2.getBrokerId())
                            .setLeader(false)
                            .setInSync(true)
                            .setBroker(
                                Resource.Relationship.create("/v3/clusters/cluster-1/brokers/2"))
                            .build(),
                        ReplicaData.builder()
                            .setMetadata(
                                Resource.Metadata.builder()
                                    .setSelf(
                                        "/v3/clusters/cluster-1/topics/topic-1/partitions/0"
                                            + "/replicas/3")
                                    .setResourceName(
                                        "crn:///kafka=cluster-1/topic=topic-1/partition=0"
                                            + "/replica=3")
                                    .build())
                            .setClusterId(CLUSTER_ID)
                            .setTopicName(TOPIC_NAME)
                            .setPartitionId(PARTITION_ID)
                            .setBrokerId(REPLICA_3.getBrokerId())
                            .setLeader(false)
                            .setInSync(false)
                            .setBroker
                                (Resource.Relationship.create("/v3/clusters/cluster-1/brokers/3"))
                            .build()))
                .build());

    assertEquals(expected, response.getValue());
  }

  @Test
  public void listReplicas_nonExistingPartitionOrTopicOrCluster_throwsNotFound() {
    expect(replicaManager.listReplicas(CLUSTER_ID, TOPIC_NAME, PARTITION_ID))
        .andReturn(failedFuture(new NotFoundException()));
    replay(replicaManager);

    FakeAsyncResponse response = new FakeAsyncResponse();
    replicasResource.listReplicas(response, CLUSTER_ID, TOPIC_NAME, PARTITION_ID);

    assertEquals(NotFoundException.class, response.getException().getClass());
  }

  @Test
  public void getReplica_existingReplica_returnsReplica() {
    expect(replicaManager.getReplica(CLUSTER_ID, TOPIC_NAME, PARTITION_ID, REPLICA_1.getBrokerId()))
        .andReturn(completedFuture(Optional.of(REPLICA_1)));
    replay(replicaManager);

    FakeAsyncResponse response = new FakeAsyncResponse();
    replicasResource.getReplica(
        response, CLUSTER_ID, TOPIC_NAME, PARTITION_ID, REPLICA_1.getBrokerId());

    GetReplicaResponse expected =
        GetReplicaResponse.create(
            ReplicaData.builder()
                .setMetadata(
                    Resource.Metadata.builder()
                        .setSelf("/v3/clusters/cluster-1/topics/topic-1/partitions/0/replicas/1")
                        .setResourceName(
                            "crn:///kafka=cluster-1/topic=topic-1/partition=0/replica=1")
                        .build())
                .setClusterId(CLUSTER_ID)
                .setTopicName(TOPIC_NAME)
                .setPartitionId(PARTITION_ID)
                .setBrokerId(REPLICA_1.getBrokerId())
                .setLeader(true)
                .setInSync(true)
                .setBroker(Resource.Relationship.create("/v3/clusters/cluster-1/brokers/1"))
                .build());

    assertEquals(expected, response.getValue());
  }

  @Test
  public void getReplica_nonExistingReplica_throwNotFound() {
    expect(replicaManager.getReplica(CLUSTER_ID, TOPIC_NAME, PARTITION_ID, REPLICA_1.getBrokerId()))
        .andReturn(completedFuture(Optional.empty()));
    replay(replicaManager);

    FakeAsyncResponse response = new FakeAsyncResponse();
    replicasResource.getReplica(
        response, CLUSTER_ID, TOPIC_NAME, PARTITION_ID, REPLICA_1.getBrokerId());

    assertEquals(NotFoundException.class, response.getException().getClass());
  }

  @Test
  public void getReplica_nonExistingPartitionOrTopicOrCluster_throwNotFound() {
    expect(replicaManager.getReplica(CLUSTER_ID, TOPIC_NAME, PARTITION_ID, REPLICA_1.getBrokerId()))
        .andReturn(failedFuture(new NotFoundException()));
    replay(replicaManager);

    FakeAsyncResponse response = new FakeAsyncResponse();
    replicasResource.getReplica(
        response, CLUSTER_ID, TOPIC_NAME, PARTITION_ID, REPLICA_1.getBrokerId());

    assertEquals(NotFoundException.class, response.getException().getClass());
  }
}
