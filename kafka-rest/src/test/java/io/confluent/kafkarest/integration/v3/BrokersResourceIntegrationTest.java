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

import io.confluent.kafkarest.entities.v3.BrokerData;
import io.confluent.kafkarest.entities.v3.BrokerDataList;
import io.confluent.kafkarest.entities.v3.GetBrokerResponse;
import io.confluent.kafkarest.entities.v3.ListBrokersResponse;
import io.confluent.kafkarest.entities.v3.Resource;
import io.confluent.kafkarest.entities.v3.Resource.Metadata;
import io.confluent.kafkarest.entities.v3.ResourceCollection;
import io.confluent.kafkarest.integration.ClusterTestHarness;
import java.util.ArrayList;
import java.util.Arrays;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import org.apache.kafka.common.Node;
import org.junit.Test;

public class BrokersResourceIntegrationTest extends ClusterTestHarness {

  public BrokersResourceIntegrationTest() {
    super(/* numBrokers= */ 3, /* withSchemaRegistry= */ false);
  }

  @Test
  public void listBrokers_existingCluster_returnsBrokers() {
    String baseUrl = restConnect;
    String clusterId = getClusterId();
    ArrayList<Node> nodes = getBrokers();

    ListBrokersResponse expected =
        ListBrokersResponse.create(
            BrokerDataList.builder()
                .setMetadata(
                    ResourceCollection.Metadata.builder()
                        .setSelf(baseUrl + "/v3/clusters/" + clusterId + "/brokers")
                        .build())
                .setData(
                    Arrays.asList(
                        BrokerData.builder()
                            .setMetadata(
                                Resource.Metadata.builder()
                                    .setSelf(
                                        baseUrl
                                            + "/v3/clusters/" + clusterId
                                            + "/brokers/" + nodes.get(0).id())
                                    .setResourceName(
                                        "crn://"
                                            + "/kafka=" + clusterId
                                            + "/broker=" + nodes.get(0).id())
                                    .build())
                            .setClusterId(clusterId)
                            .setBrokerId(nodes.get(0).id())
                            .setHost(nodes.get(0).host())
                            .setPort(nodes.get(0).port())
                            .setRack(nodes.get(0).rack())
                            .setConfigs(
                                Resource.Relationship.create(
                                    baseUrl
                                        + "/v3/clusters/" + clusterId
                                        + "/brokers/" + nodes.get(0).id()
                                        + "/configs"))
                            .setPartitionReplicas(
                                Resource.Relationship.create(
                                    baseUrl
                                        + "/v3/clusters/" + clusterId
                                        + "/brokers/" + nodes.get(0).id()
                                        + "/partition-replicas"))
                            .build(),
                        BrokerData.builder()
                            .setMetadata(
                                Metadata.builder()
                                    .setSelf(
                                        baseUrl
                                            + "/v3/clusters/" + clusterId
                                            + "/brokers/" + nodes.get(1).id())
                                    .setResourceName(
                                        "crn://"
                                            + "/kafka=" + clusterId
                                            + "/broker=" + nodes.get(1).id())
                                    .build())
                            .setClusterId(clusterId)
                            .setBrokerId(nodes.get(1).id())
                            .setHost(nodes.get(1).host())
                            .setPort(nodes.get(1).port())
                            .setRack(nodes.get(1).rack())
                            .setConfigs(
                                Resource.Relationship.create(
                                    baseUrl
                                        + "/v3/clusters/" + clusterId
                                        + "/brokers/" + nodes.get(1).id()
                                        + "/configs"))
                            .setPartitionReplicas(
                                Resource.Relationship.create(
                                    baseUrl
                                        + "/v3/clusters/" + clusterId
                                        + "/brokers/" + nodes.get(1).id()
                                        + "/partition-replicas"))
                            .build(),
                        BrokerData.builder()
                            .setMetadata(
                                Resource.Metadata.builder()
                                    .setSelf(
                                        baseUrl
                                            + "/v3/clusters/" + clusterId
                                            + "/brokers/" + nodes.get(2).id())
                                    .setResourceName(
                                        "crn://"
                                            + "/kafka=" + clusterId
                                            + "/broker=" + nodes.get(2).id())
                                    .build())
                            .setClusterId(clusterId)
                            .setBrokerId(nodes.get(2).id())
                            .setHost(nodes.get(2).host())
                            .setPort(nodes.get(2).port())
                            .setRack(nodes.get(2).rack())
                            .setConfigs(
                                Resource.Relationship.create(
                                    baseUrl
                                        + "/v3/clusters/" + clusterId
                                        + "/brokers/" + nodes.get(2).id()
                                        + "/configs"))
                            .setPartitionReplicas(
                                Resource.Relationship.create(
                                    baseUrl
                                        + "/v3/clusters/" + clusterId
                                        + "/brokers/" + nodes.get(2).id()
                                        + "/partition-replicas"))
                            .build()))
                .build());

    Response response =
        request("/v3/clusters/" + clusterId + "/brokers").accept(MediaType.APPLICATION_JSON).get();
    assertEquals(Status.OK.getStatusCode(), response.getStatus());

    ListBrokersResponse actual = response.readEntity(ListBrokersResponse.class);
    assertEquals(expected, actual);
  }

  @Test
  public void listBrokers_nonExistingCluster_returnsNotFound() {
    Response response =
        request("/v3/clusters/foobar/brokers")
            .accept(MediaType.APPLICATION_JSON).get();
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @Test
  public void getBroker_existingClusterExistingBroker_returnsBroker() {
    String baseUrl = restConnect;
    String clusterId = getClusterId();
    ArrayList<Node> nodes = getBrokers();

    GetBrokerResponse expected =
        GetBrokerResponse.create(
            BrokerData.builder()
                .setMetadata(
                    Resource.Metadata.builder()
                        .setSelf(
                            baseUrl
                                + "/v3/clusters/" + clusterId
                                + "/brokers/" + nodes.get(0).id())
                        .setResourceName(
                            "crn://"
                                + "/kafka=" + clusterId
                                + "/broker=" + nodes.get(0).id())
                        .build())
                .setClusterId(clusterId)
                .setBrokerId(nodes.get(0).id())
                .setHost(nodes.get(0).host())
                .setPort(nodes.get(0).port())
                .setRack(nodes.get(0).rack())
                .setConfigs(
                    Resource.Relationship.create(
                        baseUrl
                            + "/v3/clusters/" + clusterId
                            + "/brokers/" + nodes.get(0).id()
                            + "/configs"))
                .setPartitionReplicas(
                    Resource.Relationship.create(
                        baseUrl
                            + "/v3/clusters/" + clusterId
                            + "/brokers/" + nodes.get(0).id()
                            + "/partition-replicas")).build());

    Response response =
        request("/v3/clusters/" + clusterId + "/brokers/" + nodes.get(0).id())
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(Status.OK.getStatusCode(), response.getStatus());

    GetBrokerResponse actual = response.readEntity(GetBrokerResponse.class);
    assertEquals(expected, actual);
  }

  @Test
  public void getBroker_nonExistingCluster_returnsNotFound() {
    Response response =
        request("/v3/clusters/foobar/brokers/1")
            .accept(MediaType.APPLICATION_JSON).get();
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @Test
  public void getBroker_nonExistingBroker_returnsNotFound() {
    String clusterId = getClusterId();

    Response response =
        request("/v3/clusters/" + clusterId + "/brokers/100")
            .accept(MediaType.APPLICATION_JSON).get();
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }
}
