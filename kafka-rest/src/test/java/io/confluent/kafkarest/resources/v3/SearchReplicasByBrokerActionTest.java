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
import io.confluent.kafkarest.entities.v3.ReplicaData;
import io.confluent.kafkarest.entities.v3.ReplicaDataList;
import io.confluent.kafkarest.entities.v3.Resource;
import io.confluent.kafkarest.entities.v3.ResourceCollection;
import io.confluent.kafkarest.entities.v3.SearchReplicasByBrokerResponse;
import io.confluent.kafkarest.response.CrnFactoryImpl;
import io.confluent.kafkarest.response.FakeAsyncResponse;
import io.confluent.kafkarest.response.FakeUrlFactory;
import java.util.Arrays;
import javax.ws.rs.NotFoundException;
import org.easymock.EasyMockRule;
import org.easymock.Mock;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class SearchReplicasByBrokerActionTest {

  private static final String CLUSTER_ID = "cluster-1";
  private static final String TOPIC_NAME = "topic-1";
  private static final int BROKER_ID = 1;

  private static final PartitionReplica REPLICA_1 =
      PartitionReplica.create(
          CLUSTER_ID,
          TOPIC_NAME,
          /* partitionId= */ 1,
          /* brokerId= */ BROKER_ID,
          /* isLeader= */ true,
          /* isInSync= */ true);
  private static final PartitionReplica REPLICA_2 =
      PartitionReplica.create(
          CLUSTER_ID,
          TOPIC_NAME,
          /* partitionId= */ 2,
          /* brokerId= */ BROKER_ID,
          /* isLeader= */ false,
          /* isInSync= */ false);

  @Rule
  public final EasyMockRule mocks = new EasyMockRule(this);

  @Mock
  private ReplicaManager replicaManager;

  private SearchReplicasByBrokerAction searchReplicasByBrokerAction;

  @Before
  public void setUp() {
    searchReplicasByBrokerAction =
        new SearchReplicasByBrokerAction(
            () -> replicaManager, new CrnFactoryImpl(/* crnAuthorityConfig= */ ""),
            new FakeUrlFactory());
  }

  @Test
  public void searchReplicasByBroker_existingBroker_returnsReplicas() {
    expect(replicaManager.searchReplicasByBrokerId(CLUSTER_ID, BROKER_ID))
        .andReturn(completedFuture(Arrays.asList(REPLICA_1, REPLICA_2)));
    replay(replicaManager);

    FakeAsyncResponse response = new FakeAsyncResponse();
    searchReplicasByBrokerAction.searchReplicasByBroker(response, CLUSTER_ID, BROKER_ID);

    SearchReplicasByBrokerResponse expected =
        SearchReplicasByBrokerResponse.create(
            ReplicaDataList.builder()
                .setMetadata(
                    ResourceCollection.Metadata.builder()
                        .setSelf("/v3/clusters/cluster-1/brokers/1/partition-replicas")
                        .build())
                .setData(
                    Arrays.asList(
                        ReplicaData.builder()
                            .setMetadata(
                                Resource.Metadata.builder()
                                    .setSelf(
                                        "/v3/clusters/cluster-1/topics/topic-1/partitions/1"
                                            + "/replicas/1")
                                    .setResourceName(
                                        "crn:///kafka=cluster-1/topic=topic-1/partition=1"
                                            + "/replica=1")
                                    .build())
                            .setClusterId(CLUSTER_ID)
                            .setTopicName(TOPIC_NAME)
                            .setPartitionId(REPLICA_1.getPartitionId())
                            .setBrokerId(BROKER_ID)
                            .setLeader(true)
                            .setInSync(true)
                            .setBroker(
                                Resource.Relationship.create("/v3/clusters/cluster-1/brokers/1"))
                            .build(),
                        ReplicaData.builder()
                            .setMetadata(
                                Resource.Metadata.builder()
                                    .setSelf(
                                        "/v3/clusters/cluster-1/topics/topic-1/partitions/2"
                                            + "/replicas/1")
                                    .setResourceName(
                                        "crn:///kafka=cluster-1/topic=topic-1/partition=2"
                                            + "/replica=1")
                                    .build())
                            .setClusterId(CLUSTER_ID)
                            .setTopicName(TOPIC_NAME)
                            .setPartitionId(REPLICA_2.getPartitionId())
                            .setBrokerId(BROKER_ID)
                            .setLeader(false)
                            .setInSync(false)
                            .setBroker(
                                Resource.Relationship.create("/v3/clusters/cluster-1/brokers/1"))
                            .build()))
                .build());

    assertEquals(expected, response.getValue());
  }

  @Test
  public void searchReplicasByBroker_nonExistingBroker_returnsNotFound() {
    expect(replicaManager.searchReplicasByBrokerId(CLUSTER_ID, BROKER_ID))
        .andReturn(failedFuture(new NotFoundException()));
    replay(replicaManager);

    FakeAsyncResponse response = new FakeAsyncResponse();
    searchReplicasByBrokerAction.searchReplicasByBroker(response, CLUSTER_ID, BROKER_ID);

    assertEquals(NotFoundException.class, response.getException().getClass());
  }
}
