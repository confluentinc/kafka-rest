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
import static org.junit.Assert.assertTrue;

import io.confluent.kafkarest.entities.v3.ListAllReassignmentsResponse;
import io.confluent.kafkarest.entities.v3.ReassignmentData;
import io.confluent.kafkarest.integration.ClusterTestHarness;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import org.apache.kafka.clients.admin.NewPartitionReassignment;
import org.apache.kafka.common.TopicPartition;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ListReassignmentsByTopicActionIntegrationTest extends ClusterTestHarness {

  private static final String TOPIC_NAME = "topic-1";

  public ListReassignmentsByTopicActionIntegrationTest() {
    super(/* numBrokers= */ 6, /* withSchemaRegistry= */ false);
  }

  @Before
  public void setUp() throws Exception {
    super.setUp();
    Map<Integer, List<Integer>> replicaAssignments = createAssignment(Arrays.asList(0, 1, 2), 100);
    createTopic(TOPIC_NAME, replicaAssignments);
  }

  @Test
  public void listReassignmentsByTopic_returnsReassignments() throws Exception {
    String clusterId = getClusterId();

    Map<TopicPartition, Optional<NewPartitionReassignment>> reassignmentMap =
        createReassignment(Arrays.asList(3, 4, 5), TOPIC_NAME, 100);

    alterPartitionReassignment(reassignmentMap);

    Response response = request("/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME +
        "/partitions/-/reassignments")
        .accept(MediaType.APPLICATION_JSON)
        .get();
    assertEquals(Status.OK.getStatusCode(), response.getStatus());

    ListAllReassignmentsResponse actualReassignments =
        response.readEntity(ListAllReassignmentsResponse.class);
    for (ReassignmentData data : actualReassignments.getValue().getData()) {
      assertEquals(data.getAddingReplicas(),
          reassignmentMap.get(new TopicPartition(TOPIC_NAME,
              data.getPartitionId())).get().targetReplicas());
    }
  }

  @Test
  public void listReassignmentsByTopic_nonExistingCluster_returnsNotFound() throws Exception {

    Response response =
        request("/v3/clusters/foobar/topics/topic-1/partitions/-/reassignments")
            .accept(MediaType.APPLICATION_JSON)
            .get();

    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @Test
  public void listReassignmentsByTopic_nonExistingTopic_returnsEmpty() throws Exception {
    String clusterId = getClusterId();

    Response response =
        request("/v3/clusters/" + clusterId + "/topics/foobar/partitions/-/reassignments")
            .accept(MediaType.APPLICATION_JSON)
            .get();

    assertEquals(Status.OK.getStatusCode(), response.getStatus());
    assertTrue(
        response.readEntity(ListAllReassignmentsResponse.class).getValue().getData().isEmpty());
  }
}
