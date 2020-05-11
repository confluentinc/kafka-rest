package io.confluent.kafkarest.integration.v3;

import static org.junit.Assert.assertEquals;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafkarest.Versions;
import io.confluent.kafkarest.entities.v3.CollectionLink;
import io.confluent.kafkarest.entities.v3.ListReplicasResponse;
import io.confluent.kafkarest.entities.v3.Relationship;
import io.confluent.kafkarest.entities.v3.ReplicaData;
import io.confluent.kafkarest.entities.v3.ResourceLink;
import io.confluent.kafkarest.integration.ClusterTestHarness;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import org.junit.Before;
import org.junit.Test;

public class SearchReplicasByBrokerActionIntegrationTest  extends ClusterTestHarness {

  private static final int BROKER_ID = 0;
  private static final String TOPIC_NAME = "topic-1";

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  public SearchReplicasByBrokerActionIntegrationTest() {
    super(/* numBrokers= */ 2, /* withSchemaRegistry= */ false);
  }

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();

    HashMap<Integer, List<Integer>> replicas = new HashMap<>();
    replicas.put(/* partition= */ 0, Arrays.asList(/* leader= */ 0, 1));
    replicas.put(/* partition= */ 1, Arrays.asList(/* leader= */ 1, 0));

    createTopic(TOPIC_NAME, replicas);
  }

  @Test
  public void searchReplicasByBroker_existingBroker_returnsReplicas() throws Exception {
    String baseUrl = restConnect;
    String clusterId = getClusterId();

    ListReplicasResponse expected =
            new ListReplicasResponse(
                new CollectionLink(
                    baseUrl
                        + "/v3/clusters/" + clusterId
                        + "/brokers/" + BROKER_ID
                        + "/partition-replicas",
                    /* next= */ null),
                Arrays.asList(
                    new ReplicaData(
                        "crn:///kafka=" + clusterId
                            + "/topic=" + TOPIC_NAME
                            + "/partition=0/replica=" + BROKER_ID,
                        new ResourceLink(
                            baseUrl
                                + "/v3/clusters/" + clusterId
                                + "/topics/" + TOPIC_NAME
                                + "/partitions/0/replicas/" + BROKER_ID),
                        clusterId,
                        TOPIC_NAME,
                        /* partitionId= */ 0,
                        /* brokerId= */ BROKER_ID,
                        /* isLeader= */ true,
                        /* isInSync= */ true,
                        new Relationship(
                            baseUrl + "/v3/clusters/" + clusterId + "/brokers/" + BROKER_ID)),
                    new ReplicaData(
                        "crn:///kafka=" + clusterId
                            + "/topic=" + TOPIC_NAME
                            + "/partition=1/replica=" + BROKER_ID,
                        new ResourceLink(
                            baseUrl
                                + "/v3/clusters/" + clusterId
                                + "/topics/" + TOPIC_NAME
                                + "/partitions/1/replicas/" + BROKER_ID),
                        clusterId,
                        TOPIC_NAME,
                        /* partitionId= */ 1,
                        /* brokerId= */ BROKER_ID,
                        /* isLeader= */ false,
                        /* isInSync= */ true,
                        new Relationship(
                            baseUrl + "/v3/clusters/" + clusterId + "/brokers/" + BROKER_ID))));

    Response response =
        request("/v3/clusters/" + clusterId + "/brokers/" + BROKER_ID + "/partition-replicas")
            .accept(Versions.JSON_API)
            .get();
    assertEquals(Status.OK.getStatusCode(), response.getStatus());

    ListReplicasResponse actual =
            response.readEntity(ListReplicasResponse.class);
    assertEquals(expected, actual);
  }

  @Test
  public void searchReplicasByBroker_nonExistingBroker_returnsNotFound() {
    String clusterId = getClusterId();

    Response response =
        request("/v3/clusters/" + clusterId + "/brokers/100/partition-replicas")
            .accept(Versions.JSON_API)
            .get();
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }
}
