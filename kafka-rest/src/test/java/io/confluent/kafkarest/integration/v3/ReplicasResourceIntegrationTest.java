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

import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafkarest.Versions;
import io.confluent.kafkarest.entities.v3.CollectionLink;
import io.confluent.kafkarest.entities.v3.GetReplicaResponse;
import io.confluent.kafkarest.entities.v3.ListReplicasResponse;
import io.confluent.kafkarest.entities.v3.Relationship;
import io.confluent.kafkarest.entities.v3.ReplicaData;
import io.confluent.kafkarest.entities.v3.ResourceLink;
import io.confluent.kafkarest.integration.ClusterTestHarness;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import org.junit.Before;
import org.junit.Test;

public class ReplicasResourceIntegrationTest extends ClusterTestHarness {

  private static final String TOPIC_NAME = "topic-1";

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  public ReplicasResourceIntegrationTest() {
    super(/* numBrokers= */ 1, /* withSchemaRegistry= */ false);
  }

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();

    createTopic(TOPIC_NAME, 1, (short) 1);
  }

  @Test
  public void listReplicas_existingPartition_returnsReplicas() throws Exception {
    String baseUrl = restConnect;
    String clusterId = getClusterId();

    ListReplicasResponse expected =
            new ListReplicasResponse(
                new CollectionLink(
                    baseUrl
                        + "/v3/clusters/" + clusterId
                        + "/topics/" + TOPIC_NAME
                        + "/partitions/0/replicas",
                    /* next= */ null),
                singletonList(
                    new ReplicaData(
                        "crn:///kafka=" + clusterId
                            + "/topic=" + TOPIC_NAME
                            + "/partition=0/replica=0",
                        new ResourceLink(
                            baseUrl
                                + "/v3/clusters/" + clusterId
                                + "/topics/" + TOPIC_NAME
                                + "/partitions/0/replicas/0"),
                        clusterId,
                        TOPIC_NAME,
                        /* partitionId= */ 0,
                        /* brokerId= */ 0,
                        /* isLeader= */ true,
                        /* isInSync= */ true,
                        new Relationship(baseUrl + "/v3/clusters/" + clusterId + "/brokers/0"))));

    Response response =
        request("/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME + "/partitions/0/replicas")
            .accept(Versions.JSON_API)
            .get();
    assertEquals(Status.OK.getStatusCode(), response.getStatus());

    ListReplicasResponse actual =
            response.readEntity(ListReplicasResponse.class);
    assertEquals(expected, actual);
  }

  @Test
  public void listReplicas_nonExistingPartition_returnsNotFound() {
    String clusterId = getClusterId();

    Response response =
        request("/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME + "/partitions/100/replicas")
            .accept(Versions.JSON_API)
            .get();
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @Test
  public void listReplicas_nonExistingTopic_returnsNotFound() {
    String clusterId = getClusterId();

    Response response =
        request("/v3/clusters/" + clusterId + "/topics/foobar/partitions/0/replicas")
            .accept(Versions.JSON_API)
            .get();
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @Test
  public void listReplicas_nonExistingCluster_returnsNotFound() {
    Response response =
        request("/v3/clusters/foobar/topics/" + TOPIC_NAME + "/partitions/0/replicas")
            .accept(Versions.JSON_API)
            .get();
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @Test
  public void getReplica_existingReplica_returnsReplica() throws Exception {
    String baseUrl = restConnect;
    String clusterId = getClusterId();

    GetReplicaResponse expected =
            new GetReplicaResponse(
                new ReplicaData(
                    "crn:///kafka=" + clusterId
                        + "/topic=" + TOPIC_NAME
                        + "/partition=0/replica=0",
                    new ResourceLink(
                        baseUrl
                            + "/v3/clusters/" + clusterId
                            + "/topics/" + TOPIC_NAME
                            + "/partitions/0/replicas/0"),
                    clusterId,
                    TOPIC_NAME,
                    /* partitionId= */ 0,
                    /* brokerId= */ 0,
                    /* isLeader= */ true,
                    /* isInSync= */ true,
                    new Relationship(baseUrl + "/v3/clusters/" + clusterId + "/brokers/0")));

    Response response =
        request("/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME + "/partitions/0/replicas/0")
            .accept(Versions.JSON_API)
            .get();
    assertEquals(Status.OK.getStatusCode(), response.getStatus());

    GetReplicaResponse actual =
            response.readEntity(GetReplicaResponse.class);
    assertEquals(expected, actual);
  }

  @Test
  public void getReplica_nonExistingReplica_returnsNotFound() {
    String clusterId = getClusterId();

    Response response =
        request(
            "/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME + "/partitions/0/replicas/100")
            .accept(Versions.JSON_API)
            .get();
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @Test
  public void getReplica_nonExistingPartition_returnsNotFound() {
    String clusterId = getClusterId();

    Response response =
        request(
            "/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME + "/partitions/100/replicas/0")
            .accept(Versions.JSON_API)
            .get();
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @Test
  public void getReplica_nonExistingTopic_returnsNotFound() {
    String clusterId = getClusterId();

    Response response =
        request("/v3/clusters/" + clusterId + "/topics/foobar/partitions/0/replicas/0")
            .accept(Versions.JSON_API)
            .get();
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @Test
  public void getReplica_nonExistingCluster_returnsNotFound() {
    Response response =
        request("/v3/clusters/foobar/topics/" + TOPIC_NAME + "/partitions/0/replicas/0")
            .accept(Versions.JSON_API)
            .get();
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }
}
