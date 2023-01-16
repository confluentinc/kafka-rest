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

import io.confluent.kafkarest.entities.v3.GetReplicaResponse;
import io.confluent.kafkarest.entities.v3.ListReplicasResponse;
import io.confluent.kafkarest.entities.v3.ReplicaData;
import io.confluent.kafkarest.entities.v3.ReplicaDataList;
import io.confluent.kafkarest.entities.v3.Resource;
import io.confluent.kafkarest.entities.v3.ResourceCollection;
import io.confluent.kafkarest.integration.ClusterTestHarness;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import org.junit.Before;
import org.junit.Test;

public class ReplicasResourceIntegrationTest extends ClusterTestHarness {

  private static final String TOPIC_NAME = "topic-1";

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
  public void listReplicas_existingPartition_returnsReplicas() {
    String baseUrl = restConnect;
    String clusterId = getClusterId();

    ListReplicasResponse expected =
        ListReplicasResponse.create(
            ReplicaDataList.builder()
                .setMetadata(
                    ResourceCollection.Metadata.builder()
                        .setSelf(
                            baseUrl
                                + "/v3/clusters/" + clusterId
                                + "/topics/" + TOPIC_NAME
                                + "/partitions/0/replicas")
                        .build())
                .setData(
                    singletonList(
                        ReplicaData.builder()
                            .setMetadata(
                                Resource.Metadata.builder()
                                    .setSelf(
                                        baseUrl
                                            + "/v3/clusters/" + clusterId
                                            + "/topics/" + TOPIC_NAME
                                            + "/partitions/0/replicas/0")
                                    .setResourceName(
                                        "crn:///kafka=" + clusterId
                                            + "/topic=" + TOPIC_NAME
                                            + "/partition=0/replica=0")
                                    .build())
                            .setClusterId(clusterId)
                            .setTopicName(TOPIC_NAME)
                            .setPartitionId(0)
                            .setBrokerId(0)
                            .setLeader(true)
                            .setInSync(true)
                            .setBroker(
                                Resource.Relationship.create(
                                    baseUrl + "/v3/clusters/" + clusterId + "/brokers/0"))
                            .build()))
                .build());

    Response response =
        request("/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME + "/partitions/0/replicas")
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(Status.OK.getStatusCode(), response.getStatus());

    ListReplicasResponse actual = response.readEntity(ListReplicasResponse.class);
    assertEquals(expected, actual);
  }

  @Test
  public void listReplicas_nonExistingPartition_returnsNotFound() {
    String clusterId = getClusterId();

    Response response =
        request("/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME + "/partitions/100/replicas")
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @Test
  public void listReplicas_nonExistingTopic_returnsNotFound() {
    String clusterId = getClusterId();

    Response response =
        request("/v3/clusters/" + clusterId + "/topics/foobar/partitions/0/replicas")
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @Test
  public void listReplicas_nonExistingCluster_returnsNotFound() {
    Response response =
        request("/v3/clusters/foobar/topics/" + TOPIC_NAME + "/partitions/0/replicas")
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @Test
  public void getReplica_existingReplica_returnsReplica() {
    String baseUrl = restConnect;
    String clusterId = getClusterId();

    GetReplicaResponse expected =
        GetReplicaResponse.create(
            ReplicaData.builder()
                .setMetadata(
                    Resource.Metadata.builder()
                        .setSelf(
                            baseUrl
                                + "/v3/clusters/" + clusterId
                                + "/topics/" + TOPIC_NAME
                                + "/partitions/0/replicas/0")
                        .setResourceName(
                            "crn:///kafka=" + clusterId
                                + "/topic=" + TOPIC_NAME
                                + "/partition=0/replica=0")
                        .build())
                .setClusterId(clusterId)
                .setTopicName(TOPIC_NAME)
                .setPartitionId(0)
                .setBrokerId(0)
                .setLeader(true)
                .setInSync(true)
                .setBroker(
                    Resource.Relationship.create(
                        baseUrl + "/v3/clusters/" + clusterId + "/brokers/0"))
                .build());

    Response response =
        request("/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME + "/partitions/0/replicas/0")
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(Status.OK.getStatusCode(), response.getStatus());

    GetReplicaResponse actual = response.readEntity(GetReplicaResponse.class);
    assertEquals(expected, actual);
  }

  @Test
  public void getReplica_nonExistingReplica_returnsNotFound() {
    String clusterId = getClusterId();

    Response response =
        request(
            "/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME + "/partitions/0/replicas/100")
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @Test
  public void getReplica_nonExistingPartition_returnsNotFound() {
    String clusterId = getClusterId();

    Response response =
        request(
            "/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME + "/partitions/100/replicas/0")
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @Test
  public void getReplica_nonExistingTopic_returnsNotFound() {
    String clusterId = getClusterId();

    Response response =
        request("/v3/clusters/" + clusterId + "/topics/foobar/partitions/0/replicas/0")
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @Test
  public void getReplica_nonExistingCluster_returnsNotFound() {
    Response response =
        request("/v3/clusters/foobar/topics/" + TOPIC_NAME + "/partitions/0/replicas/0")
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }
}
