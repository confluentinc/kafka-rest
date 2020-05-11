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
import io.confluent.kafkarest.entities.v3.GetPartitionResponse;
import io.confluent.kafkarest.entities.v3.ListPartitionsResponse;
import io.confluent.kafkarest.entities.v3.PartitionData;
import io.confluent.kafkarest.entities.v3.Relationship;
import io.confluent.kafkarest.entities.v3.ResourceLink;
import io.confluent.kafkarest.integration.ClusterTestHarness;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import org.junit.Before;
import org.junit.Test;

public class PartitionsResourceIntegrationTest extends ClusterTestHarness {

  private static final String TOPIC_NAME = "topic-1";

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  public PartitionsResourceIntegrationTest() {
    super(/* numBrokers= */ 1, /* withSchemaRegistry= */ false);
  }

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();

    createTopic(TOPIC_NAME, 1, (short) 1);
  }

  @Test
  public void listPartitions_existingTopic_returnPartitions() throws Exception {
    String baseUrl = restConnect;
    String clusterId = getClusterId();

    ListPartitionsResponse expected =
            new ListPartitionsResponse(
                new CollectionLink(
                    baseUrl
                        + "/v3/clusters/" + clusterId
                        + "/topics/" + TOPIC_NAME
                        + "/partitions",
                    /* next= */ null),
                singletonList(
                    new PartitionData(
                        "crn:///kafka=" + clusterId + "/topic=" + TOPIC_NAME + "/partition=0",
                        new ResourceLink(
                            baseUrl
                            + "/v3/clusters/" + clusterId
                            + "/topics/" + TOPIC_NAME
                            + "/partitions/0"),
                        clusterId,
                        TOPIC_NAME,
                        /* partitionId= */ 0,
                        new Relationship(
                            baseUrl
                             + "/v3/clusters/" + clusterId
                            + "/topics/" + TOPIC_NAME
                            + "/partitions/0/replicas/0"),
                        new Relationship(
                            baseUrl
                                + "/v3/clusters/" + clusterId
                                + "/topics/" + TOPIC_NAME
                                + "/partitions/0/replicas"))));

    Response response =
        request("/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME + "/partitions")
            .accept(Versions.JSON_API)
            .get();
    assertEquals(Status.OK.getStatusCode(), response.getStatus());

    ListPartitionsResponse actual =
            response.readEntity(ListPartitionsResponse.class);
    assertEquals(expected, actual);
  }

  @Test
  public void listPartitions_nonExistingTopic_returnsNotFound() {
    String clusterId = getClusterId();

    Response response =
        request("/v3/clusters/" + clusterId + "/topics/foobar/partitions")
            .accept(Versions.JSON_API)
            .get();
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @Test
  public void listPartitions_nonExistingCluster_returnsNotFound() {
    Response response =
        request("/v3/clusters/foobar/topics/"+ TOPIC_NAME + "/partitions")
            .accept(Versions.JSON_API)
            .get();
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @Test
  public void getPartition_existingPartition_returnPartition() throws Exception {
    String baseUrl = restConnect;
    String clusterId = getClusterId();

    GetPartitionResponse expected =
            new GetPartitionResponse(
                new PartitionData(
                    "crn:///kafka=" + clusterId + "/topic=" + TOPIC_NAME + "/partition=0",
                    new ResourceLink(
                        baseUrl
                            + "/v3/clusters/" + clusterId
                            + "/topics/" + TOPIC_NAME
                            + "/partitions/0"),
                    clusterId,
                    TOPIC_NAME,
                    /* partitionId= */ 0,
                    new Relationship(
                        baseUrl
                            + "/v3/clusters/" + clusterId
                            + "/topics/" + TOPIC_NAME
                            + "/partitions/0/replicas/0"),
                    new Relationship(
                        baseUrl
                            + "/v3/clusters/" + clusterId
                            + "/topics/" + TOPIC_NAME
                            + "/partitions/0/replicas")));

    Response response =
        request("/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME + "/partitions/0")
            .accept(Versions.JSON_API)
            .get();
    assertEquals(Status.OK.getStatusCode(), response.getStatus());

    GetPartitionResponse actual =
            response.readEntity(GetPartitionResponse.class);
    assertEquals(expected, actual);
  }

  @Test
  public void getPartition_nonExistingPartition_returnsNotFound() {
    String clusterId = getClusterId();

    Response response =
        request("/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME + "/partitions/100")
            .accept(Versions.JSON_API)
            .get();
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @Test
  public void getPartition_nonExistingTopic_returnsNotFound() {
    String clusterId = getClusterId();

    Response response =
        request("/v3/clusters/" + clusterId + "/topics/foobar/partitions/0")
            .accept(Versions.JSON_API)
            .get();
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @Test
  public void getPartition_nonExistingCluster_returnsNotFound() {
    Response response =
        request("/v3/clusters/foobar/topics/" + TOPIC_NAME + "/partitions/0")
            .accept(Versions.JSON_API)
            .get();
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }
}
