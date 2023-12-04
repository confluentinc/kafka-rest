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

import static io.confluent.kafkarest.TestUtils.TEST_WITH_PARAMETERIZED_QUORUM_NAME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.confluent.kafkarest.entities.v3.ReassignmentData;
import io.confluent.kafkarest.entities.v3.SearchReassignmentsByTopicResponse;
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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class SearchReassignmentsByTopicActionIntegrationTest extends ClusterTestHarness {

  private static final String TOPIC_NAME = "topic-1";

  public SearchReassignmentsByTopicActionIntegrationTest() {
    super(/* numBrokers= */ 6, /* withSchemaRegistry= */ false);
  }

  @BeforeEach
  public void setUp(TestInfo testInfo) throws Exception {
    super.setUp(testInfo);
    Map<Integer, List<Integer>> replicaAssignments = createAssignment(Arrays.asList(0, 1, 2), 100);
    createTopic(TOPIC_NAME, replicaAssignments);
  }

  @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
  @ValueSource(strings = {"kraft", "zk"})
  public void searchReassignmentsByTopic_returnsReassignments(String quorum) throws Exception {
    String clusterId = getClusterId();

    Map<TopicPartition, Optional<NewPartitionReassignment>> reassignmentMap =
        createReassignment(Arrays.asList(3, 4, 5), TOPIC_NAME, 100);

    alterPartitionReassignment(reassignmentMap);

    Response response =
        request(
                "/v3/clusters/"
                    + clusterId
                    + "/topics/"
                    + TOPIC_NAME
                    + "/partitions/-/reassignment")
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(Status.OK.getStatusCode(), response.getStatus());

    SearchReassignmentsByTopicResponse actualReassignments =
        response.readEntity(SearchReassignmentsByTopicResponse.class);
    for (ReassignmentData data : actualReassignments.getValue().getData()) {
      assertEquals(
          data.getAddingReplicas(),
          reassignmentMap
              .get(new TopicPartition(TOPIC_NAME, data.getPartitionId()))
              .get()
              .targetReplicas());
    }
  }

  @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
  @ValueSource(strings = {"kraft", "zk"})
  public void searchReassignmentsByTopic_nonExistingCluster_returnsNotFound(String quorum)
      throws Exception {

    Response response =
        request("/v3/clusters/foobar/topics/topic-1/partitions/-/reassignment")
            .accept(MediaType.APPLICATION_JSON)
            .get();

    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
  @ValueSource(strings = {"kraft", "zk"})
  public void searchReassignmentsByTopic_nonExistingTopic_returnsEmpty(String quorum)
      throws Exception {
    String clusterId = getClusterId();

    Response response =
        request("/v3/clusters/" + clusterId + "/topics/foobar/partitions/-/reassignment")
            .accept(MediaType.APPLICATION_JSON)
            .get();

    assertEquals(Status.OK.getStatusCode(), response.getStatus());
    assertTrue(
        response
            .readEntity(SearchReassignmentsByTopicResponse.class)
            .getValue()
            .getData()
            .isEmpty());
  }
}
