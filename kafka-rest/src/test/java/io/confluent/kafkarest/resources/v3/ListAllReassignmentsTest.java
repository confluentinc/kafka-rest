package io.confluent.kafkarest.resources.v3;

import static io.confluent.kafkarest.common.CompletableFutures.failedFuture;
import static java.util.Arrays.asList;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.junit.Assert.assertEquals;

import io.confluent.kafkarest.controllers.ReassignmentManager;
import io.confluent.kafkarest.entities.Broker;
import io.confluent.kafkarest.entities.Cluster;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import javax.ws.rs.NotFoundException;
import org.apache.kafka.clients.admin.PartitionReassignment;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.easymock.EasyMockRule;
import org.easymock.Mock;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

public class ListAllReassignmentsTest {

  private static final String CLUSTER_ID = "cluster-1";

  private static final Node NODE_1 = new Node(1, "broker-1", 9091);
  private static final Node NODE_2 = new Node(2, "broker-2", 9092);
  private static final Node NODE_3 = new Node(3, "broker-3", 9093);

  private static final Broker BROKER_1 = Broker.fromNode(CLUSTER_ID, NODE_1);
  private static final Broker BROKER_2 = Broker.fromNode(CLUSTER_ID, NODE_2);
  private static final Broker BROKER_3 = Broker.fromNode(CLUSTER_ID, NODE_3);

  private static final String TOPIC_1 = "topic-1";
  private static final int PARTITION_ID_1 = 1;
  private static final int PARTITION_ID_2 = 2;
  private static final int PARTITION_ID_3 = 3;


  private static final Cluster CLUSTER =
      Cluster.create(CLUSTER_ID, BROKER_1, asList(BROKER_1, BROKER_2, BROKER_3));

  private static final List<Integer> REPLICAS_1 = Arrays.asList(1, 2, 3, 4, 5);
  private static final List<Integer> REPLICAS_2 = Arrays.asList(1, 2, 3, 4);
  private static final List<Integer> REPLICAS_3 = Arrays.asList(4, 5, 6);

  private static final List<Integer> ADDING_REPLICAS_1 = Arrays.asList(1, 2, 3);
  private static final List<Integer> ADDING_REPLICAS_2 = Arrays.asList(1, 2, 3);
  private static final List<Integer> ADDING_REPLICAS_3 = Arrays.asList(5, 6);

  private static final List<Integer> REMOVING_REPLICAS_1 = Arrays.asList(4, 5);
  private static final List<Integer> REMOVING_REPLICAS_2 = Arrays.asList(4);
  private static final List<Integer> REMOVING_REPLICAS_3 = Arrays.asList(4);


  private static final TopicPartition TOPIC_PARTITION_1 = new TopicPartition(TOPIC_1,
      PARTITION_ID_1);
  private static final TopicPartition TOPIC_PARTITION_2 = new TopicPartition(TOPIC_1,
      PARTITION_ID_2);
  private static final TopicPartition TOPIC_PARTITION_3 = new TopicPartition(TOPIC_1,
      PARTITION_ID_3);

  private static final PartitionReassignment PARTITION_REASSIGNMENT_1 =
      new PartitionReassignment(REPLICAS_1, ADDING_REPLICAS_1, REMOVING_REPLICAS_1);
  private static final PartitionReassignment PARTITION_REASSIGNMENT_2 =
      new PartitionReassignment(REPLICAS_2, ADDING_REPLICAS_2, REMOVING_REPLICAS_2);
  private static final PartitionReassignment PARTITION_REASSIGNMENT_3 =
      new PartitionReassignment(REPLICAS_3, ADDING_REPLICAS_3, REMOVING_REPLICAS_3);

  private static final Map<TopicPartition, PartitionReassignment> REASSIGNMENT_MAP =
      new HashMap<>();

  private static final Reassignment REASSIGNMENT_1 = new Reassignment(CLUSTER_ID, TOPIC_1,
      PARTITION_ID_1, REPLICAS_1, ADDING_REPLICAS_1, REMOVING_REPLICAS_1);
  private static final Reassignment REASSIGNMENT_2 = new Reassignment(CLUSTER_ID, TOPIC_1,
      PARTITION_ID_2, REPLICAS_2, ADDING_REPLICAS_2, REMOVING_REPLICAS_2);
  private static final Reassignment REASSIGNMENT_3 = new Reassignment(CLUSTER_ID, TOPIC_1,
      PARTITION_ID_3, REPLICAS_3, ADDING_REPLICAS_3, REMOVING_REPLICAS_3);


  @Rule
  public final EasyMockRule mocks = new EasyMockRule(this);

  @Mock
  private ReassignmentManager reassignmentManager;

  private ListAllReassignments listAllReassignments;

  @Before
  public void setUp() {
    listAllReassignments = new ListAllReassignments(
        () -> reassignmentManager,
        new CrnFactoryImpl(/* crnAuthorityConfig= */ ""),
        new FakeUrlFactory());
  }

  @Test
  public void listAllReassignments_existingCluster_returnsReassignments() {
    expect(reassignmentManager.listReassignments(CLUSTER_ID))
        .andReturn(
            CompletableFuture.completedFuture(
                asList(REASSIGNMENT_1, REASSIGNMENT_2, REASSIGNMENT_3)));
    replay(reassignmentManager);

    FakeAsyncResponse response = new FakeAsyncResponse();
    listAllReassignments.listReassignments(response, CLUSTER_ID);

    ListReassignmentsResponse expected =
        new ListReassignmentsResponse(
            new CollectionLink("/v3/clusters/cluster-1/topics/-/partitions/-/reassignments",
                /* next= */ null),
            Arrays.asList(
                new ReassignmentData(
                    "crn:///kafka=cluster-1/topic=topic-1/partition=1/reassignment=1",
                    new ResourceLink("/v3/clusters/cluster-1/topics/topic-1/partitions/1"
                        + "/reassignments/1"),
                    CLUSTER_ID,
                    TOPIC_1,
                    PARTITION_ID_1,
                    new Relationship(
                        "/v3/clusters/cluster-1/topics/topic-1/partitions/1/replicas")),
                new ReassignmentData(
                    "crn:///kafka=cluster-1/topic=topic-1/partition=2/reassignment=2",
                    new ResourceLink("/v3/clusters/cluster-1/topics/topic-1/partitions/2"
                        + "/reassignments/2"),
                    CLUSTER_ID,
                    TOPIC_1,
                    PARTITION_ID_2,
                    new Relationship(
                        "/v3/clusters/cluster-1/topics/topic-1/partitions/2/replicas")),
                new ReassignmentData(
                    "crn:///kafka=cluster-1/topic=topic-1/partition=3/reassignment=3",
                    new ResourceLink("/v3/clusters/cluster-1/topics/topic-1/partitions/3"
                        + "/reassignments/3"),
                    CLUSTER_ID,
                    TOPIC_1,
                    PARTITION_ID_3,
                    new Relationship(
                        "/v3/clusters/cluster-1/topics/topic-1/partitions/3/replicas"))));

    assertEquals(expected, response.getValue());
  }

  @Test
  public void listPartitions_nonExistingCluster_throwsNotFound() {
    expect(reassignmentManager.listReassignments(CLUSTER_ID))
        .andReturn(failedFuture(new NotFoundException()));
    replay(reassignmentManager);

    FakeAsyncResponse response = new FakeAsyncResponse();
    listAllReassignments.listReassignments(response, CLUSTER_ID);

    assertEquals(NotFoundException.class, response.getException().getClass());
  }

}
