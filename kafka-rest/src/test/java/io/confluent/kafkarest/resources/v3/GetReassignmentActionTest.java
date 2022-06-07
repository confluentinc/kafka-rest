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
import static org.junit.jupiter.api.Assertions.assertEquals;

import io.confluent.kafkarest.controllers.ReassignmentManager;
import io.confluent.kafkarest.entities.Reassignment;
import io.confluent.kafkarest.entities.v3.GetReassignmentResponse;
import io.confluent.kafkarest.entities.v3.ReassignmentData;
import io.confluent.kafkarest.entities.v3.Resource;
import io.confluent.kafkarest.response.CrnFactoryImpl;
import io.confluent.kafkarest.response.FakeAsyncResponse;
import io.confluent.kafkarest.response.FakeUrlFactory;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import javax.ws.rs.NotFoundException;
import org.easymock.EasyMockExtension;
import org.easymock.Mock;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(EasyMockExtension.class)
public class GetReassignmentActionTest {

  private static final String CLUSTER_ID = "cluster-1";
  private static final String TOPIC_1 = "topic-1";

  private static final int PARTITION_ID_1 = 1;

  private static final List<Integer> ADDING_REPLICAS_1 = Arrays.asList(1, 2, 3);

  private static final List<Integer> REMOVING_REPLICAS_1 = Arrays.asList(4, 5);

  private static final Reassignment REASSIGNMENT_1 =
      Reassignment.create(
          CLUSTER_ID, TOPIC_1, PARTITION_ID_1, ADDING_REPLICAS_1, REMOVING_REPLICAS_1);

  @Mock private ReassignmentManager reassignmentManager;

  private GetReassignmentAction getReassignmentAction;

  @BeforeEach
  public void setUp() {
    getReassignmentAction =
        new GetReassignmentAction(
            () -> reassignmentManager,
            new CrnFactoryImpl(/* crnAuthorityConfig= */ ""),
            new FakeUrlFactory());
  }

  @Test
  public void getReassignment_existingClusterTopicPartition_returnsReassignment() throws Exception {
    expect(reassignmentManager.getReassignment(CLUSTER_ID, TOPIC_1, PARTITION_ID_1))
        .andReturn(CompletableFuture.completedFuture(Optional.of(REASSIGNMENT_1)));
    replay(reassignmentManager);

    FakeAsyncResponse response = new FakeAsyncResponse();

    getReassignmentAction.getReassignment(response, CLUSTER_ID, TOPIC_1, PARTITION_ID_1);

    GetReassignmentResponse expected =
        GetReassignmentResponse.create(
            ReassignmentData.builder()
                .setMetadata(
                    Resource.Metadata.builder()
                        .setSelf(
                            "/v3/clusters/cluster-1/topics/topic-1/partitions/1" + "/reassignment")
                        .setResourceName(
                            "crn:///kafka=cluster-1/topic=topic-1/partition=1/reassignment")
                        .build())
                .setClusterId(CLUSTER_ID)
                .setTopicName(TOPIC_1)
                .setPartitionId(PARTITION_ID_1)
                .setAddingReplicas(ADDING_REPLICAS_1)
                .setRemovingReplicas(REMOVING_REPLICAS_1)
                .setReplicas(
                    Resource.Relationship.create(
                        "/v3/clusters/cluster-1/topics/topic-1/partitions/1/replicas"))
                .build());

    assertEquals(expected, response.getValue());
  }

  @Test
  public void getReassignment_nonExistingCluster_throwsNotFound() {
    expect(reassignmentManager.getReassignment(CLUSTER_ID, TOPIC_1, 1))
        .andReturn(failedFuture(new NotFoundException()));
    replay(reassignmentManager);

    FakeAsyncResponse response = new FakeAsyncResponse();
    getReassignmentAction.getReassignment(response, CLUSTER_ID, "topic-1", 1);

    assertEquals(NotFoundException.class, response.getException().getClass());
  }

  @Test
  public void getReassignment_nonExistingTopic_throwsNotFound() {
    expect(reassignmentManager.getReassignment(CLUSTER_ID, TOPIC_1, 1))
        .andReturn(failedFuture(new NotFoundException()));
    replay(reassignmentManager);

    FakeAsyncResponse response = new FakeAsyncResponse();
    getReassignmentAction.getReassignment(response, CLUSTER_ID, "topic-1", 1);

    assertEquals(NotFoundException.class, response.getException().getClass());
  }

  @Test
  public void getReassignment_nonExistingPartition_throwsNotFound() {
    expect(reassignmentManager.getReassignment(CLUSTER_ID, TOPIC_1, 1))
        .andReturn(failedFuture(new NotFoundException()));
    replay(reassignmentManager);

    FakeAsyncResponse response = new FakeAsyncResponse();
    getReassignmentAction.getReassignment(response, CLUSTER_ID, "topic-1", 1);

    assertEquals(NotFoundException.class, response.getException().getClass());
  }
}
