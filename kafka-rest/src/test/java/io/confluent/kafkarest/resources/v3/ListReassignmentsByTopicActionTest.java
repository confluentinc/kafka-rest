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
import io.confluent.kafkarest.entities.v3.CollectionLink;
import io.confluent.kafkarest.entities.v3.ListReassignmentsResponse;
import io.confluent.kafkarest.entities.v3.ReassignmentData;
import io.confluent.kafkarest.entities.v3.Relationship;
import io.confluent.kafkarest.entities.v3.ResourceLink;
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

public class ListReassignmentsByTopicActionTest {

  private static final String CLUSTER_ID = "cluster-1";
  private static final String TOPIC_1 = "topic-1";

  private static final int PARTITION_ID_1 = 1;
  private static final int PARTITION_ID_2 = 2;
  private static final int PARTITION_ID_3 = 3;

  private static final List<Integer> REPLICAS_1 = Arrays.asList(1, 2, 3, 4, 5);
  private static final List<Integer> REPLICAS_2 = Arrays.asList(1, 2, 3, 4);
  private static final List<Integer> REPLICAS_3 = Arrays.asList(4, 5, 6);

  private static final List<Integer> ADDING_REPLICAS_1 = Arrays.asList(1, 2, 3);
  private static final List<Integer> ADDING_REPLICAS_2 = Arrays.asList(1, 2, 3);
  private static final List<Integer> ADDING_REPLICAS_3 = Arrays.asList(5, 6);

  private static final List<Integer> REMOVING_REPLICAS_1 = Arrays.asList(4, 5);
  private static final List<Integer> REMOVING_REPLICAS_2 = Arrays.asList(4);
  private static final List<Integer> REMOVING_REPLICAS_3 = Arrays.asList(4);

  private static final Reassignment REASSIGNMENT_1 = Reassignment.create(CLUSTER_ID, TOPIC_1,
      PARTITION_ID_1, REPLICAS_1, ADDING_REPLICAS_1, REMOVING_REPLICAS_1);
  private static final Reassignment REASSIGNMENT_2 = Reassignment.create(CLUSTER_ID, TOPIC_1,
      PARTITION_ID_2, REPLICAS_2, ADDING_REPLICAS_2, REMOVING_REPLICAS_2);
  private static final Reassignment REASSIGNMENT_3 = Reassignment.create(CLUSTER_ID, TOPIC_1,
      PARTITION_ID_3, REPLICAS_3, ADDING_REPLICAS_3, REMOVING_REPLICAS_3);

  @Rule
  public final EasyMockRule mocks = new EasyMockRule(this);

  @Mock
  private ReassignmentManager reassignmentManager;

  private ListReassignmentsByTopicAction listReassignmentsByTopicAction;

  @Before
  public void setUp() {
    listReassignmentsByTopicAction = new ListReassignmentsByTopicAction(
        () -> reassignmentManager,
        new CrnFactoryImpl(/* crnAuthorityConfig= */ ""),
        new FakeUrlFactory());
  }

  @Test
  public void listAllReassignments_existingCluster_returnsReassignments() {
    expect(reassignmentManager.listReassignmentsByTopicName(CLUSTER_ID, TOPIC_1))
        .andReturn(
            CompletableFuture.completedFuture(
                asList(REASSIGNMENT_1, REASSIGNMENT_2, REASSIGNMENT_3)));
    replay(reassignmentManager);

    FakeAsyncResponse response = new FakeAsyncResponse();
    listReassignmentsByTopicAction.listReassignmentsByTopic(response, CLUSTER_ID, TOPIC_1);

    ListReassignmentsResponse expected =
        new ListReassignmentsResponse(
            new CollectionLink("/v3/clusters/cluster-1/topics/topic-1/partitions/-/reassignments",
                /* next= */ null),
            Arrays.asList(
                new ReassignmentData(
                    "crn:///kafka=cluster-1/topic=topic-1/partition=1/reassignment=1",
                    new ResourceLink("/v3/clusters/cluster-1/topics/topic-1/partitions/1"
                        + "/reassignments/1"),
                    CLUSTER_ID,
                    TOPIC_1,
                    PARTITION_ID_1,
                    REPLICAS_1,
                    ADDING_REPLICAS_1,
                    REMOVING_REPLICAS_1,
                    new Relationship(
                        "/v3/clusters/cluster-1/topics/topic-1/partitions/1/replicas")),
                new ReassignmentData(
                    "crn:///kafka=cluster-1/topic=topic-1/partition=2/reassignment=2",
                    new ResourceLink("/v3/clusters/cluster-1/topics/topic-1/partitions/2"
                        + "/reassignments/2"),
                    CLUSTER_ID,
                    TOPIC_1,
                    PARTITION_ID_2,
                    REPLICAS_2,
                    ADDING_REPLICAS_2,
                    REMOVING_REPLICAS_2,
                    new Relationship(
                        "/v3/clusters/cluster-1/topics/topic-1/partitions/2/replicas")),
                new ReassignmentData(
                    "crn:///kafka=cluster-1/topic=topic-1/partition=3/reassignment=3",
                    new ResourceLink("/v3/clusters/cluster-1/topics/topic-1/partitions/3"
                        + "/reassignments/3"),
                    CLUSTER_ID,
                    TOPIC_1,
                    PARTITION_ID_3,
                    REPLICAS_3,
                    ADDING_REPLICAS_3,
                    REMOVING_REPLICAS_3,
                    new Relationship(
                        "/v3/clusters/cluster-1/topics/topic-1/partitions/3/replicas"))));

    assertEquals(expected, response.getValue());
  }

  @Test
  public void listAllReassignments_nonExistingCluster_throwsNotFound() {
    expect(reassignmentManager.listReassignmentsByTopicName(CLUSTER_ID, TOPIC_1))
        .andReturn(failedFuture(new NotFoundException()));
    replay(reassignmentManager);

    FakeAsyncResponse response = new FakeAsyncResponse();
    listReassignmentsByTopicAction.listReassignmentsByTopic(response, CLUSTER_ID, TOPIC_1);

    assertEquals(NotFoundException.class, response.getException().getClass());
  }
}
