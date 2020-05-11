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

import static org.junit.Assert.assertEquals;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafkarest.Versions;
import io.confluent.kafkarest.entities.v3.BrokerData;
import io.confluent.kafkarest.entities.v3.CollectionLink;
import io.confluent.kafkarest.entities.v3.GetBrokerResponse;
import io.confluent.kafkarest.entities.v3.ListBrokersResponse;
import io.confluent.kafkarest.entities.v3.Relationship;
import io.confluent.kafkarest.entities.v3.ResourceLink;
import io.confluent.kafkarest.integration.ClusterTestHarness;
import java.util.ArrayList;
import java.util.Arrays;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import org.apache.kafka.common.Node;
import org.junit.Test;

public class BrokersResourceIntegrationTest extends ClusterTestHarness {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  public BrokersResourceIntegrationTest() {
    super(/* numBrokers= */ 3, /* withSchemaRegistry= */ false);
  }

  @Test
  public void listBrokers_existingCluster_returnsBrokers() throws Exception {
    String baseUrl = restConnect;
    String clusterId = getClusterId();
    ArrayList<Node> nodes = getBrokers();

    ListBrokersResponse expected =
            new ListBrokersResponse(
                new CollectionLink(
                    baseUrl + "/v3/clusters/" + clusterId + "/brokers", /* next= */ null),
                Arrays.asList(
                    new BrokerData(
                        "crn:///kafka=" + clusterId + "/broker=" + nodes.get(0).id(),
                        new ResourceLink(
                            baseUrl
                                + "/v3/clusters/" + clusterId
                                + "/brokers/" + nodes.get(0).id()),
                        clusterId,
                        nodes.get(0).id(),
                        nodes.get(0).host(),
                        nodes.get(0).port(),
                        nodes.get(0).rack(),
                        new Relationship(
                            baseUrl
                                + "/v3/clusters/" + clusterId
                                + "/brokers/" + nodes.get(0).id()
                                + "/configs"),
                        new Relationship(
                            baseUrl
                                + "/v3/clusters/" + clusterId
                                + "/brokers/" + nodes.get(0).id()
                                + "/partition-replicas")),
                    new BrokerData(
                        "crn:///kafka=" + clusterId + "/broker=" + nodes.get(1).id(),
                        new ResourceLink(
                            baseUrl
                                + "/v3/clusters/" + clusterId
                                + "/brokers/" + nodes.get(1).id()),
                        clusterId,
                        nodes.get(1).id(),
                        nodes.get(1).host(),
                        nodes.get(1).port(),
                        nodes.get(1).rack(),
                        new Relationship(
                            baseUrl
                                + "/v3/clusters/" + clusterId
                                + "/brokers/" + nodes.get(1).id()
                                + "/configs"),
                        new Relationship(
                            baseUrl
                                + "/v3/clusters/" + clusterId
                                + "/brokers/" + nodes.get(1).id()
                                + "/partition-replicas")),
                    new BrokerData(
                        "crn:///kafka=" + clusterId + "/broker=" + nodes.get(2).id(),
                        new ResourceLink(
                            baseUrl
                                + "/v3/clusters/" + clusterId
                                + "/brokers/" + nodes.get(2).id()),
                        clusterId,
                        nodes.get(2).id(),
                        nodes.get(2).host(),
                        nodes.get(2).port(),
                        nodes.get(2).rack(),
                        new Relationship(
                            baseUrl
                                + "/v3/clusters/" + clusterId
                                + "/brokers/" + nodes.get(2).id()
                                + "/configs"),
                        new Relationship(
                            baseUrl
                                + "/v3/clusters/" + clusterId
                                + "/brokers/" + nodes.get(2).id()
                                + "/partition-replicas"))));

    Response response =
        request("/v3/clusters/" + clusterId + "/brokers").accept(Versions.JSON_API).get();
    assertEquals(Status.OK.getStatusCode(), response.getStatus());

    ListBrokersResponse actual =
            response.readEntity(ListBrokersResponse.class);
    assertEquals(expected, actual);
  }

  @Test
  public void listBrokers_nonExistingCluster_returnsNotFound() {
    Response response = request("/v3/clusters/foobar/brokers").accept(Versions.JSON_API).get();
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @Test
  public void getBroker_existingClusterExistingBroker_returnsBroker() throws Exception {
    String baseUrl = restConnect;
    String clusterId = getClusterId();
    ArrayList<Node> nodes = getBrokers();

    GetBrokerResponse expected =
            new GetBrokerResponse(
                new BrokerData(
                    "crn:///kafka=" + clusterId + "/broker=" + nodes.get(0).id(),
                    new ResourceLink(
                        baseUrl
                            + "/v3/clusters/" + clusterId
                            + "/brokers/" + nodes.get(0).id()),
                    clusterId,
                    nodes.get(0).id(),
                    nodes.get(0).host(),
                    nodes.get(0).port(),
                    nodes.get(0).rack(),
                    new Relationship(
                        baseUrl
                            + "/v3/clusters/" + clusterId
                            + "/brokers/" + nodes.get(0).id()
                            + "/configs"),
                    new Relationship(
                        baseUrl
                            + "/v3/clusters/" + clusterId
                            + "/brokers/" + nodes.get(0).id()
                            + "/partition-replicas")));

    Response response =
        request("/v3/clusters/" + clusterId + "/brokers/" + nodes.get(0).id())
            .accept(Versions.JSON_API)
            .get();
    assertEquals(Status.OK.getStatusCode(), response.getStatus());

    GetBrokerResponse actual =
            response.readEntity(GetBrokerResponse.class);
    assertEquals(expected, actual);
  }

  @Test
  public void getBroker_nonExistingCluster_returnsNotFound() {
    Response response = request("/v3/clusters/foobar/brokers/1").accept(Versions.JSON_API).get();
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @Test
  public void getBroker_nonExistingBroker_returnsNotFound() {
    String clusterId = getClusterId();

    Response response =
        request("/v3/clusters/" + clusterId + "/brokers/100").accept(Versions.JSON_API).get();
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }
}
