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

package io.confluent.kafkarest.integration.v3;

import static org.junit.Assert.assertEquals;

import io.confluent.kafkarest.Versions;
import io.confluent.kafkarest.entities.v3.GetReassignmentResponse;
import io.confluent.kafkarest.entities.v3.ReassignmentData;
import io.confluent.kafkarest.integration.ClusterTestHarness;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import org.apache.kafka.clients.admin.NewPartitionReassignment;
import org.apache.kafka.common.TopicPartition;
import org.junit.Before;
import org.junit.Test;

public class GetReassignmentActionIntegrationTest extends ClusterTestHarness {

  private static final String TOPIC_NAME = "topic-1";
  private static final int PARTITION_ID = 1;

  public GetReassignmentActionIntegrationTest() {
    super(/* numBrokers= */ 6, /* withSchemaRegistry= */ false);
  }

  @Before
  public void setUp() throws Exception {
    super.setUp();
    Map<Integer, List<Integer>> replicaAssignments =
        ListAllReassignmentsActionIntegrationTest.createAssignment(Arrays.asList(0, 1, 2));
    createTopic(TOPIC_NAME, replicaAssignments);
  }

  @Test
  public void getReassignment_returnsReassignment() throws Exception {
    String clusterId = getClusterId();

    Map<TopicPartition, Optional<NewPartitionReassignment>> reassignmentMap =
        ListAllReassignmentsActionIntegrationTest.createReassignment(Arrays.asList(3, 4, 5));

    alterPartitionReassignment(reassignmentMap);

    Response response = request("/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME +
        "/partitions/" + PARTITION_ID + "/reassignments")
        .accept(Versions.JSON_API)
        .get();

    assertEquals(Status.OK.getStatusCode(), response.getStatus());

    ReassignmentData actualReassignment =
        response.readEntity(GetReassignmentResponse.class).getData();
    assertEquals(actualReassignment.getAttributes().getAddingReplicas(),
        reassignmentMap.get(new TopicPartition(TOPIC_NAME,
            actualReassignment.getAttributes().getPartitionId())).get().targetReplicas());
  }

  @Test
  public void getReassignments_nonExistingCluster_returnsNotFound() {

    Response response =
        request("/v3/clusters/foobar/topics/" + TOPIC_NAME + "/partitions/" + PARTITION_ID
            + "/reassignments")
            .accept(Versions.JSON_API)
            .get();
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @Test
  public void getReassignments_nonExistingTopic_returnsNotFound() {
    String clusterId = getClusterId();

    Response response = request("/v3/clusters/" + clusterId + "/topics/foobar/partitions/"
        + PARTITION_ID + "/reassignments")
        .accept(Versions.JSON_API)
        .get();
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @Test
  public void getReassignments_nonExistingPartition_returnsNotFound() {
    String clusterId = getClusterId();

    Response response = request("/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME
        + "/partitions/10/reassignments")
        .accept(Versions.JSON_API)
        .get();
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }
}
