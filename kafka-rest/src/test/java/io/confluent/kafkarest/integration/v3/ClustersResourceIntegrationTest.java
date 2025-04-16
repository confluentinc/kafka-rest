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

import static io.confluent.kafkarest.TestUtils.TEST_WITH_PARAMETERIZED_QUORUM_NAME;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableList;
import io.confluent.kafkarest.entities.v3.ClusterData;
import io.confluent.kafkarest.entities.v3.ClusterDataList;
import io.confluent.kafkarest.entities.v3.GetClusterResponse;
import io.confluent.kafkarest.entities.v3.ListClustersResponse;
import io.confluent.kafkarest.entities.v3.Resource;
import io.confluent.kafkarest.entities.v3.ResourceCollection;
import io.confluent.kafkarest.integration.ClusterTestHarness;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.Response.Status;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class ClustersResourceIntegrationTest extends ClusterTestHarness {

  public ClustersResourceIntegrationTest() {
    super(/* numBrokers= */ 3, /* withSchemaRegistry= */ false);
  }

  @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
  @ValueSource(strings = {"kraft"})
  public void listClusters_returnsArrayWithOwnCluster(String quorum) {
    String baseUrl = restConnect;
    String clusterId = getClusterId();

    Response response = request("/v3/clusters").accept(MediaType.APPLICATION_JSON).get();
    assertEquals(Status.OK.getStatusCode(), response.getStatus());

    ListClustersResponse actual = response.readEntity(ListClustersResponse.class);

    ImmutableList<ClusterData> actualClusterData = actual.getValue().getData();
    assertEquals(1, actualClusterData.size());
    Resource.Relationship relationship = actualClusterData.get(0).getController().orElse(null);
    assertNotNull(relationship);
    assertTrue(
        relationship.getRelated().startsWith(baseUrl + "/v3/clusters/" + clusterId + "/brokers/"));

    ListClustersResponse expected =
        ListClustersResponse.create(
            ClusterDataList.builder()
                .setMetadata(
                    ResourceCollection.Metadata.builder().setSelf(baseUrl + "/v3/clusters").build())
                .setData(
                    singletonList(
                        ClusterData.builder()
                            .setMetadata(
                                Resource.Metadata.builder()
                                    .setSelf(baseUrl + "/v3/clusters/" + clusterId)
                                    .setResourceName("crn:///kafka=" + clusterId)
                                    .build())
                            .setClusterId(clusterId)
                            .setController(relationship)
                            .setAcls(
                                Resource.Relationship.create(
                                    baseUrl + "/v3/clusters/" + clusterId + "/acls"))
                            .setBrokers(
                                Resource.Relationship.create(
                                    baseUrl + "/v3/clusters/" + clusterId + "/brokers"))
                            .setBrokerConfigs(
                                Resource.Relationship.create(
                                    baseUrl + "/v3/clusters/" + clusterId + "/broker-configs"))
                            .setConsumerGroups(
                                Resource.Relationship.create(
                                    baseUrl + "/v3/clusters/" + clusterId + "/consumer-groups"))
                            .setTopics(
                                Resource.Relationship.create(
                                    baseUrl + "/v3/clusters/" + clusterId + "/topics"))
                            .setPartitionReassignments(
                                Resource.Relationship.create(
                                    baseUrl
                                        + "/v3/clusters/"
                                        + clusterId
                                        + "/topics/-/partitions/-/reassignment"))
                            .build()))
                .build());
    assertEquals(expected, actual);
  }

  @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
  @ValueSource(strings = {"kraft"})
  public void getCluster_ownCluster_returnsOwnCluster(String quorum) {
    String baseUrl = restConnect;
    String clusterId = getClusterId();

    Response response =
        request(String.format("/v3/clusters/%s", clusterId))
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(Status.OK.getStatusCode(), response.getStatus());

    GetClusterResponse actual = response.readEntity(GetClusterResponse.class);

    ClusterData actualClusterData = actual.getValue();
    Resource.Relationship relationship = actualClusterData.getController().orElse(null);
    assertNotNull(relationship);
    assertTrue(
        relationship.getRelated().startsWith(baseUrl + "/v3/clusters/" + clusterId + "/brokers/"));

    GetClusterResponse expected =
        GetClusterResponse.create(
            ClusterData.builder()
                .setMetadata(
                    Resource.Metadata.builder()
                        .setSelf(baseUrl + "/v3/clusters/" + clusterId)
                        .setResourceName("crn:///kafka=" + clusterId)
                        .build())
                .setClusterId(clusterId)
                .setController(relationship)
                .setAcls(
                    Resource.Relationship.create(baseUrl + "/v3/clusters/" + clusterId + "/acls"))
                .setBrokers(
                    Resource.Relationship.create(
                        baseUrl + "/v3/clusters/" + clusterId + "/brokers"))
                .setBrokerConfigs(
                    Resource.Relationship.create(
                        baseUrl + "/v3/clusters/" + clusterId + "/broker-configs"))
                .setConsumerGroups(
                    Resource.Relationship.create(
                        baseUrl + "/v3/clusters/" + clusterId + "/consumer-groups"))
                .setTopics(
                    Resource.Relationship.create(baseUrl + "/v3/clusters/" + clusterId + "/topics"))
                .setPartitionReassignments(
                    Resource.Relationship.create(
                        baseUrl
                            + "/v3/clusters/"
                            + clusterId
                            + "/topics/-/partitions/-/reassignment"))
                .build());
    assertEquals(expected, actual);
  }

  @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
  @ValueSource(strings = {"kraft"})
  public void getCluster_differentCluster_returnsNotFound(String quorum) {
    Response response = request("/v3/clusters/foobar").accept(MediaType.APPLICATION_JSON).get();
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }
}
