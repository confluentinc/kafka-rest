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
import io.confluent.kafkarest.entities.v3.ClusterData;
import io.confluent.kafkarest.entities.v3.CollectionLink;
import io.confluent.kafkarest.entities.v3.GetClusterResponse;
import io.confluent.kafkarest.entities.v3.ListClustersResponse;
import io.confluent.kafkarest.entities.v3.Relationship;
import io.confluent.kafkarest.entities.v3.ResourceLink;
import io.confluent.kafkarest.integration.ClusterTestHarness;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import org.junit.Test;

public class ClustersResourceIntegrationTest extends ClusterTestHarness {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  public ClustersResourceIntegrationTest() {
    super(/* numBrokers= */ 3, /* withSchemaRegistry= */ false);
  }

  @Test
  public void listClusters_returnsArrayWithOwnCluster() throws Exception {
    String baseUrl = restConnect;
    String clusterId = getClusterId();
    int controllerId = getControllerID();

    ListClustersResponse expected =
            new ListClustersResponse(
                new CollectionLink(baseUrl + "/v3/clusters", /* next= */ null),
                singletonList(
                    new ClusterData(
                        "crn:///kafka=" + clusterId,
                        new ResourceLink(baseUrl + "/v3/clusters/" + clusterId),
                        clusterId,
                        new Relationship(
                            baseUrl + "/v3/clusters/" + clusterId + "/brokers/" + controllerId),
                        new Relationship(baseUrl + "/v3/clusters/" + clusterId + "/brokers"),
                        new Relationship(baseUrl + "/v3/clusters/" + clusterId + "/topics"))));

    Response response = request("/v3/clusters").accept(Versions.JSON_API).get();
    assertEquals(Status.OK.getStatusCode(), response.getStatus());

    ListClustersResponse actual =
            response.readEntity(ListClustersResponse.class);
    assertEquals(expected, actual);
  }

  @Test
  public void getCluster_ownCluster_returnsOwnCluster() throws Exception {
    String baseUrl = restConnect;
    String clusterId = getClusterId();
    int controllerId = getControllerID();

    GetClusterResponse expected =
            new GetClusterResponse(
                new ClusterData(
                    "crn:///kafka=" + clusterId,
                    new ResourceLink(baseUrl + "/v3/clusters/" + clusterId),
                    clusterId,
                    new Relationship(
                        baseUrl + "/v3/clusters/" + clusterId + "/brokers/" + controllerId),
                    new Relationship(baseUrl + "/v3/clusters/" + clusterId + "/brokers"),
                    new Relationship(baseUrl + "/v3/clusters/" + clusterId + "/topics")));

    Response response =
        request(String.format("/v3/clusters/%s", clusterId))
            .accept(Versions.JSON_API)
            .get();
    assertEquals(Status.OK.getStatusCode(), response.getStatus());

    GetClusterResponse actual =
            response.readEntity(GetClusterResponse.class);
    assertEquals(expected, actual);
  }

  @Test
  public void getCluster_differentCluster_returnsNotFound() {
    Response response = request("/v3/clusters/foobar").accept(Versions.JSON_API).get();
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }
}
