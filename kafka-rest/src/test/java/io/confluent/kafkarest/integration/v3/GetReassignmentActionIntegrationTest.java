package io.confluent.kafkarest.integration.v3;

import static org.junit.Assert.assertEquals;

import io.confluent.kafkarest.Versions;
import io.confluent.kafkarest.entities.v3.GetReassignmentResponse;
import io.confluent.kafkarest.entities.v3.ReassignmentData;
import io.confluent.kafkarest.integration.ClusterTestHarness;
import java.util.ArrayList;
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

public class GetReassignmentActionIntegrationTest extends ClusterTestHarness {

  private static final String TOPIC_NAME = "topic-1";
  private static final int PARTITION_ID = 1;
  private static final int TOTAL_REPLICAS = 50;

  public GetReassignmentActionIntegrationTest() {
    super(/* numBrokers= */ 100, /* withSchemaRegistry= */ false);
  }

  @Before
  public void setUp() throws Exception {
    super.setUp();
    Map<Integer, List<Integer>> replicaAssignments = createAssignment();
    createTopic(TOPIC_NAME, replicaAssignments);
  }

  @Test
  public void getReassignment_returnsReassignment() throws Exception {
    String clusterId = getClusterId();

    Map<TopicPartition, Optional<NewPartitionReassignment>> reassignmentMap =
        createReassignment(50);

    alterPartitionReassignment(reassignmentMap);

    Response response = request("/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME +
        "/partitions" + PARTITION_ID + "reassignments")
        .accept(Versions.JSON_API)
        .get();

    assertEquals(Status.OK.getStatusCode(), response.getStatus());
  }

  private Map<Integer, List<Integer>> createAssignment() {
    List<Integer> replicas = new ArrayList<>();
    Map<Integer, List<Integer>> replicaAssignments = new HashMap<>();

    for (int i = 0; i < TOTAL_REPLICAS; i++) {
      replicas.add(i);
    }
    replicaAssignments.put(PARTITION_ID, replicas);

    return replicaAssignments;
  }

  private Map<TopicPartition, Optional<NewPartitionReassignment>> createReassignment(int brokerId) {
    List<Integer> replicas = new ArrayList<>();
    for (int i = brokerId; i < 100; i++) {
      replicas.add(i);
    }

    Map<TopicPartition, Optional<NewPartitionReassignment>> reassignmentMap = new HashMap<>();
    reassignmentMap.put(new TopicPartition(TOPIC_NAME, PARTITION_ID),
        Optional.of(new NewPartitionReassignment(replicas)));

    return reassignmentMap;
  }

}
