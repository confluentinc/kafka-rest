package io.confluent.kafkarest.resources.v3;

import static io.confluent.kafkarest.common.CompletableFutures.failedFuture;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.junit.Assert.assertEquals;

import io.confluent.kafkarest.controllers.ReplicaManager;
import io.confluent.kafkarest.entities.PartitionReplica;
import io.confluent.kafkarest.entities.v3.CollectionLink;
import io.confluent.kafkarest.entities.v3.ListReplicasResponse;
import io.confluent.kafkarest.entities.v3.Relationship;
import io.confluent.kafkarest.entities.v3.ReplicaData;
import io.confluent.kafkarest.entities.v3.ResourceLink;
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

    ListReplicasResponse expected =
        new ListReplicasResponse(
            new CollectionLink(
                "/v3/clusters/cluster-1/brokers/1/partition-replicas", /* next= */ null),
            Arrays.asList(
                new ReplicaData(
                    "crn:///kafka=cluster-1/topic=topic-1/partition=1/replica=1",
                    new ResourceLink(
                        "/v3/clusters/cluster-1/topics/topic-1/partitions/1/replicas/1"),
                    CLUSTER_ID,
                    TOPIC_NAME,
                    REPLICA_1.getPartitionId(),
                    BROKER_ID,
                    /* isLeader= */ true,
                    /* isInSync= */ true,
                    new Relationship("/v3/clusters/cluster-1/brokers/1")),
                new ReplicaData(
                    "crn:///kafka=cluster-1/topic=topic-1/partition=2/replica=1",
                    new ResourceLink(
                        "/v3/clusters/cluster-1/topics/topic-1/partitions/2/replicas/1"),
                    CLUSTER_ID,
                    TOPIC_NAME,
                    REPLICA_2.getPartitionId(),
                    BROKER_ID,
                    /* isLeader= */ false,
                    /* isInSync= */ false,
                    new Relationship("/v3/clusters/cluster-1/brokers/1"))));

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
