/*
 * Copyright 2021 Confluent Inc.
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

import io.confluent.kafkarest.controllers.TopicManager;
import io.confluent.kafkarest.entities.Partition;
import io.confluent.kafkarest.entities.PartitionReplica;
import io.confluent.kafkarest.entities.Topic;
import io.confluent.kafkarest.entities.v3.*;
import io.confluent.kafkarest.response.CrnFactoryImpl;
import io.confluent.kafkarest.response.FakeAsyncResponse;
import io.confluent.kafkarest.response.FakeUrlFactory;
import org.easymock.EasyMockRule;
import org.easymock.Mock;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import javax.ws.rs.NotFoundException;
import java.util.ArrayList;
import java.util.Arrays;

import static io.confluent.kafkarest.common.CompletableFutures.failedFuture;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.junit.Assert.assertEquals;

@RunWith(JUnit4.class)
public class ListAllReplicasActionTest {

  private static final String CLUSTER_ID = "cluster-1";
  private static final String TOPIC_NAME = "topic-1";

  private static final PartitionReplica REPLICA_1 =
      PartitionReplica.create(
          CLUSTER_ID,
          TOPIC_NAME,
          0,
          1,
          /* isLeader= */ true,
          /* isInSync= */ true);
  private static final PartitionReplica REPLICA_2 =
          PartitionReplica.create(
                  CLUSTER_ID,
                  TOPIC_NAME,
                  0,
                  0,
                  /* isLeader= */ false,
                  /* isInSync= */ true);
  private static final PartitionReplica REPLICA_3 =
          PartitionReplica.create(
                  CLUSTER_ID,
                  TOPIC_NAME,
                  1,
                  0,
                  /* isLeader= */ true,
                  /* isInSync= */ true);
  private static final PartitionReplica REPLICA_4 =
          PartitionReplica.create(
                  CLUSTER_ID,
                  TOPIC_NAME,
                  1,
                  1,
                  /* isLeader= */ false,
                  /* isInSync= */ false);

  private static final Partition PARTITION_1 =
          Partition.create(
                  CLUSTER_ID,
                  TOPIC_NAME,
                  0,
                  Arrays.asList(REPLICA_1,REPLICA_2));
  private static final Partition PARTITION_2 =
          Partition.create(
                  CLUSTER_ID,
                  TOPIC_NAME,
                  1,
                  Arrays.asList(REPLICA_3,REPLICA_4));

  @Rule
  public final EasyMockRule mocks = new EasyMockRule(this);

  @Mock
  private TopicManager topicManager;

  private ListAllReplicasAction allReplicasResource;

  @Before
  public void setUp() {
    allReplicasResource =
        new ListAllReplicasAction(
            () -> topicManager,
            new CrnFactoryImpl(/* crnAuthorityConfig= */ ""),
            new FakeUrlFactory());
  }

  @Test
  public void listAllReplicas_existingTopic_returnsReplicas() {
    expect(topicManager.listTopics(CLUSTER_ID))
        .andReturn(
            completedFuture(Arrays.asList(Topic.create(
                CLUSTER_ID,
                TOPIC_NAME,
                Arrays.asList(PARTITION_1,PARTITION_2),
                (short) 1,
                false
            )))
        );

    replay(topicManager);

    FakeAsyncResponse response = new FakeAsyncResponse();
    allReplicasResource.listReplicas(response, CLUSTER_ID);

    ListReplicasResponse expected =
            ListReplicasResponse.create(
                    ReplicaDataList.builder()
                            .setMetadata(
                                    ResourceCollection.Metadata.builder()
                                            .setSelf(
                                                    "/v3/clusters/cluster-1/topics/-/replicas")
                                            .build())
                            .setData(
                                    Arrays.asList(
                                            ReplicaData.builder()
                                                    .setMetadata(
                                                            Resource.Metadata.builder()
                                                                    .setSelf(
                                                                            "/v3/clusters/cluster-1/topics/topic-1/partitions/0/replicas/1")
                                                                    .setResourceName(
                                                                            "crn:///kafka=cluster-1/topic=topic-1/partition=0/replica=1")
                                                                    .build())
                                                    .setClusterId(CLUSTER_ID)
                                                    .setTopicName(TOPIC_NAME)
                                                    .setBrokerId(1)
                                                    .setPartitionId(0)
                                                    .setInSync(true)
                                                    .setLeader(true)
                                                    .setBroker(Resource.Relationship.create("/v3/clusters/cluster-1/brokers/1"))
                                                    .build(),
                                            ReplicaData.builder()
                                                    .setMetadata(
                                                            Resource.Metadata.builder()
                                                                    .setSelf(
                                                                            "/v3/clusters/cluster-1/topics/topic-1/partitions/0/replicas/0")
                                                                    .setResourceName(
                                                                            "crn:///kafka=cluster-1/topic=topic-1/partition=0/replica=0")
                                                                    .build())
                                                    .setClusterId(CLUSTER_ID)
                                                    .setTopicName(TOPIC_NAME)
                                                    .setBrokerId(0)
                                                    .setPartitionId(0)
                                                    .setInSync(true)
                                                    .setLeader(false)
                                                    .setBroker(Resource.Relationship.create("/v3/clusters/cluster-1/brokers/0"))
                                                    .build(),
                                            ReplicaData.builder()
                                                    .setMetadata(
                                                            Resource.Metadata.builder()
                                                                    .setSelf(
                                                                            "/v3/clusters/cluster-1/topics/topic-1/partitions/1/replicas/0")
                                                                    .setResourceName(
                                                                            "crn:///kafka=cluster-1/topic=topic-1/partition=1/replica=0")
                                                                    .build())
                                                    .setClusterId(CLUSTER_ID)
                                                    .setTopicName(TOPIC_NAME)
                                                    .setBrokerId(0)
                                                    .setPartitionId(1)
                                                    .setInSync(true)
                                                    .setLeader(true)
                                                    .setBroker(Resource.Relationship.create("/v3/clusters/cluster-1/brokers/0"))
                                                    .build(),
                                            ReplicaData.builder()
                                                    .setMetadata(
                                                            Resource.Metadata.builder()
                                                                    .setSelf(
                                                                            "/v3/clusters/cluster-1/topics/topic-1/partitions/1/replicas/1")
                                                                    .setResourceName(
                                                                            "crn:///kafka=cluster-1/topic=topic-1/partition=1/replica=1")
                                                                    .build())
                                                    .setClusterId(CLUSTER_ID)
                                                    .setTopicName(TOPIC_NAME)
                                                    .setBrokerId(1)
                                                    .setPartitionId(1)
                                                    .setInSync(false)
                                                    .setLeader(false)
                                                    .setBroker(Resource.Relationship.create("/v3/clusters/cluster-1/brokers/1"))
                                                    .build()
                                    ))
                            .build());

    assertEquals(expected, response.getValue());
  }

  @Test
  public void listAllReplicas_noTopics_returnsEmptyReplicas() {
    expect(topicManager.listTopics(CLUSTER_ID))
        .andReturn(
            completedFuture(new ArrayList<>())
        );

    replay(topicManager);

    FakeAsyncResponse response = new FakeAsyncResponse();
    allReplicasResource.listReplicas(response, CLUSTER_ID);

    ListReplicasResponse expected =
        ListReplicasResponse.create(
            ReplicaDataList.builder()
                .setMetadata(
                    ResourceCollection.Metadata.builder()
                        .setSelf(
                            "/v3/clusters/cluster-1/topics/-/replicas")
                        .build())
                .setData(new ArrayList<>())
                .build());

    assertEquals(expected, response.getValue());
  }

  @Test
  public void listAllReplicas_nonExistingCluster_throwsNotFound() {
    expect(topicManager.listTopics(CLUSTER_ID))
        .andReturn(failedFuture(new NotFoundException()));
    replay(topicManager);

    FakeAsyncResponse response = new FakeAsyncResponse();
    allReplicasResource.listReplicas(response, CLUSTER_ID);

    assertEquals(NotFoundException.class, response.getException().getClass());
  }

}
