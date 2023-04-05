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
import static java.util.Arrays.asList;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.junit.Assert.assertEquals;

import io.confluent.kafkarest.controllers.ReassignmentManager;
import io.confluent.kafkarest.entities.Reassignment;
import io.confluent.kafkarest.entities.v3.ReassignmentData;
import io.confluent.kafkarest.entities.v3.ReassignmentDataList;
import io.confluent.kafkarest.entities.v3.Resource;
import io.confluent.kafkarest.entities.v3.ResourceCollection;
import io.confluent.kafkarest.entities.v3.SearchReassignmentsByTopicResponse;
import io.confluent.kafkarest.response.CrnFactoryImpl;
import io.confluent.kafkarest.response.FakeAsyncResponse;
import io.confluent.kafkarest.response.FakeUrlFactory;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import javax.ws.rs.NotFoundException;
import org.easymock.EasyMockRule;
import org.easymock.Mock;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

public class SearchReassignmentsByTopicActionTest {

  private static final String CLUSTER_ID = "cluster-1";
  private static final String TOPIC_1 = "topic-1";

  private static final int PARTITION_ID_1 = 1;
  private static final int PARTITION_ID_2 = 2;
  private static final int PARTITION_ID_3 = 3;

  private static final List<Integer> ADDING_REPLICAS_1 = Arrays.asList(1, 2, 3);
  private static final List<Integer> ADDING_REPLICAS_2 = Arrays.asList(1, 2, 3);
  private static final List<Integer> ADDING_REPLICAS_3 = Arrays.asList(5, 6);

  private static final List<Integer> REMOVING_REPLICAS_1 = Arrays.asList(4, 5);
  private static final List<Integer> REMOVING_REPLICAS_2 = Arrays.asList(4);
  private static final List<Integer> REMOVING_REPLICAS_3 = Arrays.asList(4);

  private static final Reassignment REASSIGNMENT_1 = Reassignment.create(CLUSTER_ID, TOPIC_1,
      PARTITION_ID_1, ADDING_REPLICAS_1, REMOVING_REPLICAS_1);
  private static final Reassignment REASSIGNMENT_2 = Reassignment.create(CLUSTER_ID, TOPIC_1,
      PARTITION_ID_2, ADDING_REPLICAS_2, REMOVING_REPLICAS_2);
  private static final Reassignment REASSIGNMENT_3 = Reassignment.create(CLUSTER_ID, TOPIC_1,
      PARTITION_ID_3, ADDING_REPLICAS_3, REMOVING_REPLICAS_3);

  @Rule
  public final EasyMockRule mocks = new EasyMockRule(this);

  @Mock
  private ReassignmentManager reassignmentManager;

  private SearchReassignmentsByTopicAction listReassignmentsByTopicAction;

  @Before
  public void setUp() {
    listReassignmentsByTopicAction = new SearchReassignmentsByTopicAction(
        () -> reassignmentManager,
        new CrnFactoryImpl(/* crnAuthorityConfig= */ ""),
        new FakeUrlFactory());
  }

  @Test
  public void searchReassignments_existingCluster_returnsReassignments() {
    expect(reassignmentManager.searchReassignmentsByTopicName(CLUSTER_ID, TOPIC_1))
        .andReturn(
            CompletableFuture.completedFuture(
                asList(REASSIGNMENT_1, REASSIGNMENT_2, REASSIGNMENT_3)));
    replay(reassignmentManager);

    FakeAsyncResponse response = new FakeAsyncResponse();
    listReassignmentsByTopicAction.listReassignmentsByTopic(response, CLUSTER_ID, TOPIC_1);

    SearchReassignmentsByTopicResponse expected =
        SearchReassignmentsByTopicResponse.create(
            ReassignmentDataList.builder()
                .setMetadata(
                    ResourceCollection.Metadata.builder()
                        .setSelf("/v3/clusters/cluster-1/topics/topic-1/partitions/-/reassignments")
                        .build())
                .setData(
                    Arrays.asList(
                        ReassignmentData.builder()
                            .setMetadata(
                                Resource.Metadata.builder()
                                    .setSelf(
                                        "/v3/clusters/cluster-1/topics/topic-1/partitions/1"
                                            + "/reassignments")
                                    .setResourceName(
                                        "crn:///kafka=cluster-1/topic=topic-1/partition=1"
                                            + "/reassignments")
                                    .build())
                            .setClusterId(CLUSTER_ID)
                            .setTopicName(TOPIC_1)
                            .setPartitionId(PARTITION_ID_1)
                            .setAddingReplicas(ADDING_REPLICAS_1)
                            .setRemovingReplicas(REMOVING_REPLICAS_1)
                            .setReplicas(
                                Resource.Relationship.create(
                                    "/v3/clusters/cluster-1/topics/topic-1/partitions/1/replicas"))
                            .build(),
                        ReassignmentData.builder()
                            .setMetadata(
                                Resource.Metadata.builder()
                                    .setSelf(
                                        "/v3/clusters/cluster-1/topics/topic-1/partitions/2"
                                            + "/reassignments")
                                    .setResourceName(
                                        "crn:///kafka=cluster-1/topic=topic-1/partition=2"
                                            + "/reassignments")
                                    .build())
                            .setClusterId(CLUSTER_ID)
                            .setTopicName(TOPIC_1)
                            .setPartitionId(PARTITION_ID_2)
                            .setAddingReplicas(ADDING_REPLICAS_2)
                            .setRemovingReplicas(REMOVING_REPLICAS_2)
                            .setReplicas(
                                Resource.Relationship.create(
                                    "/v3/clusters/cluster-1/topics/topic-1/partitions/2/replicas"))
                            .build(),
                        ReassignmentData.builder()
                            .setMetadata(
                                Resource.Metadata.builder()
                                    .setSelf(
                                        "/v3/clusters/cluster-1/topics/topic-1/partitions/3"
                                            + "/reassignments")
                                    .setResourceName(
                                        "crn:///kafka=cluster-1/topic=topic-1/partition=3"
                                            + "/reassignments")
                                    .build())
                            .setClusterId(CLUSTER_ID)
                            .setTopicName(TOPIC_1)
                            .setPartitionId(PARTITION_ID_3)
                            .setAddingReplicas(ADDING_REPLICAS_3)
                            .setRemovingReplicas(REMOVING_REPLICAS_3)
                            .setReplicas(
                                Resource.Relationship.create(
                                    "/v3/clusters/cluster-1/topics/topic-1/partitions/3/replicas"))
                            .build()))
                .build());

    assertEquals(expected, response.getValue());
  }

  @Test
  public void searchReassignments_nonExistingCluster_throwsNotFound() {
    expect(reassignmentManager.searchReassignmentsByTopicName(CLUSTER_ID, TOPIC_1))
        .andReturn(failedFuture(new NotFoundException()));
    replay(reassignmentManager);

    FakeAsyncResponse response = new FakeAsyncResponse();
    listReassignmentsByTopicAction.listReassignmentsByTopic(response, CLUSTER_ID, TOPIC_1);

    assertEquals(NotFoundException.class, response.getException().getClass());
  }
}
