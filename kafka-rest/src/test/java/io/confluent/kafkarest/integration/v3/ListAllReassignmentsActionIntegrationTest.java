package io.confluent.kafkarest.integration.v3;

import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;

import io.confluent.kafkarest.Versions;
import io.confluent.kafkarest.entities.Reassignment;
import io.confluent.kafkarest.entities.v3.CollectionLink;
import io.confluent.kafkarest.entities.v3.ListReassignmentsResponse;
import io.confluent.kafkarest.entities.v3.ReassignmentData;
import io.confluent.kafkarest.entities.v3.Relationship;
import io.confluent.kafkarest.entities.v3.ResourceLink;
import io.confluent.kafkarest.integration.ClusterTestHarness;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import org.apache.kafka.clients.admin.NewPartitionReassignment;
import org.apache.kafka.common.TopicPartition;
import org.junit.Before;
import org.junit.Test;

public class ListAllReassignmentsActionIntegrationTest extends ClusterTestHarness {

  private static final String TOPIC_NAME = "topic-1";

  public ListAllReassignmentsActionIntegrationTest() {
    super(/* numBrokers= */ 6, /* withSchemaRegistry= */ false);
  }

  @Before
  public void setUp() throws Exception {
    super.setUp();
    Map<Integer, List<Integer>> replicaAssignments = createAssignment(Arrays.asList(0, 1, 2));
    createTopic(TOPIC_NAME, replicaAssignments);
  }

  @Test
  public void listAllReassignments_returnsReassignments() throws Exception {
    String clusterId = getClusterId();

    Map<TopicPartition, Optional<NewPartitionReassignment>> reassignmentMap =
        createReassignment(Arrays.asList(3, 4, 5));

    alterPartitionReassignment(reassignmentMap);

    Response response = request("/v3/clusters/" + clusterId + "/topics/-/partitions"
        + "/-/reassignments")
        .accept(Versions.JSON_API)
        .get();
    assertEquals(Status.OK.getStatusCode(), response.getStatus());

    List<ReassignmentData> actualReassignments =
        response.readEntity(ListReassignmentsResponse.class).getData();
    for (ReassignmentData data : actualReassignments) {
      assertEquals(data.getAttributes().getAddingReplicas(),
          reassignmentMap.get(new TopicPartition(TOPIC_NAME,
              data.getAttributes().getPartitionId())).get().targetReplicas());
    }
  }

  private Map<Integer, List<Integer>> createAssignment(List<Integer> replicaIds) {
    Map<Integer, List<Integer>> replicaAssignments = new HashMap<>();
    for (int i = 0; i < 100; i++) {
      replicaAssignments.put(i, replicaIds);
    }
    return replicaAssignments;
  }

  private Map<TopicPartition, Optional<NewPartitionReassignment>> createReassignment(
      List<Integer> replicaIds) {
    Map<TopicPartition, Optional<NewPartitionReassignment>> reassignmentMap = new HashMap<>();
    for (int i = 0; i < 100; i++) {
      reassignmentMap.put(new TopicPartition(TOPIC_NAME, i),
          Optional.of(new NewPartitionReassignment(replicaIds)));
    }
    return reassignmentMap;
  }

}
