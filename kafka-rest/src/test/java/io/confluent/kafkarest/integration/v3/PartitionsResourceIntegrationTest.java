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

import io.confluent.kafkarest.entities.v3.GetPartitionResponse;
import io.confluent.kafkarest.entities.v3.ListPartitionsResponse;
import io.confluent.kafkarest.entities.v3.PartitionData;
import io.confluent.kafkarest.entities.v3.PartitionDataList;
import io.confluent.kafkarest.entities.v3.Resource;
import io.confluent.kafkarest.entities.v3.ResourceCollection;
import io.confluent.kafkarest.integration.ClusterTestHarness;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class PartitionsResourceIntegrationTest extends ClusterTestHarness {

  private static final String TOPIC_NAME = "topic-1";

  public PartitionsResourceIntegrationTest() {
    super(/* numBrokers= */ 1, /* withSchemaRegistry= */ false);
  }

  @BeforeEach
  @Override
  public void setUp(TestInfo testInfo) throws Exception {
    super.setUp(testInfo);

    createTopic(TOPIC_NAME, 1, (short) 1);
  }

  @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
  @ValueSource(strings = {"kraft", "zk"})
  public void listPartitions_existingTopic_returnPartitions(String quorum) {
    String baseUrl = restConnect;
    String clusterId = getClusterId();

    ListPartitionsResponse expected =
        ListPartitionsResponse.create(
            PartitionDataList.builder()
                .setMetadata(
                    ResourceCollection.Metadata.builder()
                        .setSelf(
                            baseUrl
                                + "/v3/clusters/"
                                + clusterId
                                + "/topics/"
                                + TOPIC_NAME
                                + "/partitions")
                        .build())
                .setData(
                    singletonList(
                        PartitionData.builder()
                            .setMetadata(
                                Resource.Metadata.builder()
                                    .setSelf(
                                        baseUrl
                                            + "/v3/clusters/"
                                            + clusterId
                                            + "/topics/"
                                            + TOPIC_NAME
                                            + "/partitions/0")
                                    .setResourceName(
                                        "crn://"
                                            + "/kafka="
                                            + clusterId
                                            + "/topic="
                                            + TOPIC_NAME
                                            + "/partition=0")
                                    .build())
                            .setClusterId(clusterId)
                            .setTopicName(TOPIC_NAME)
                            .setPartitionId(0)
                            .setLeader(
                                Resource.Relationship.create(
                                    baseUrl
                                        + "/v3/clusters/"
                                        + clusterId
                                        + "/topics/"
                                        + TOPIC_NAME
                                        + "/partitions/0/replicas/0"))
                            .setReplicas(
                                Resource.Relationship.create(
                                    baseUrl
                                        + "/v3/clusters/"
                                        + clusterId
                                        + "/topics/"
                                        + TOPIC_NAME
                                        + "/partitions/0/replicas"))
                            .setReassignment(
                                Resource.Relationship.create(
                                    baseUrl
                                        + "/v3/clusters/"
                                        + clusterId
                                        + "/topics/"
                                        + TOPIC_NAME
                                        + "/partitions/0/reassignment"))
                            .build()))
                .build());

    Response response =
        request("/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME + "/partitions")
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(Status.OK.getStatusCode(), response.getStatus());

    ListPartitionsResponse actual = response.readEntity(ListPartitionsResponse.class);
    assertEquals(expected, actual);
  }

  @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
  @ValueSource(strings = {"kraft", "zk"})
  public void listPartitions_nonExistingTopic_returnsNotFound(String quorum) {
    String clusterId = getClusterId();

    Response response =
        request("/v3/clusters/" + clusterId + "/topics/foobar/partitions")
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
  @ValueSource(strings = {"kraft", "zk"})
  public void listPartitions_nonExistingCluster_returnsNotFound(String quorum) {
    Response response =
        request("/v3/clusters/foobar/topics/" + TOPIC_NAME + "/partitions")
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
  @ValueSource(strings = {"kraft", "zk"})
  public void getPartition_existingPartition_returnPartition(String quorum) throws Exception {
    String baseUrl = restConnect;
    String clusterId = getClusterId();

    GetPartitionResponse expected =
        GetPartitionResponse.create(
            PartitionData.builder()
                .setMetadata(
                    Resource.Metadata.builder()
                        .setSelf(
                            baseUrl
                                + "/v3/clusters/"
                                + clusterId
                                + "/topics/"
                                + TOPIC_NAME
                                + "/partitions/0")
                        .setResourceName(
                            "crn:///kafka=" + clusterId + "/topic=" + TOPIC_NAME + "/partition=0")
                        .build())
                .setClusterId(clusterId)
                .setTopicName(TOPIC_NAME)
                .setPartitionId(0)
                .setLeader(
                    Resource.Relationship.create(
                        baseUrl
                            + "/v3/clusters/"
                            + clusterId
                            + "/topics/"
                            + TOPIC_NAME
                            + "/partitions/0/replicas/0"))
                .setReplicas(
                    Resource.Relationship.create(
                        baseUrl
                            + "/v3/clusters/"
                            + clusterId
                            + "/topics/"
                            + TOPIC_NAME
                            + "/partitions/0/replicas"))
                .setReassignment(
                    Resource.Relationship.create(
                        baseUrl
                            + "/v3/clusters/"
                            + clusterId
                            + "/topics/"
                            + TOPIC_NAME
                            + "/partitions/0/reassignment"))
                .build());

    Response response =
        request("/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME + "/partitions/0")
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(Status.OK.getStatusCode(), response.getStatus());

    GetPartitionResponse actual = response.readEntity(GetPartitionResponse.class);
    assertEquals(expected, actual);
  }

  @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
  @ValueSource(strings = {"kraft", "zk"})
  public void getPartition_nonExistingPartition_returnsNotFound(String quorum) {
    String clusterId = getClusterId();

    Response response =
        request("/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME + "/partitions/100")
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
  @ValueSource(strings = {"kraft", "zk"})
  public void getPartition_nonExistingTopic_returnsNotFound(String quorum) {
    String clusterId = getClusterId();

    Response response =
        request("/v3/clusters/" + clusterId + "/topics/foobar/partitions/0")
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
  @ValueSource(strings = {"kraft", "zk"})
  public void getPartition_nonExistingCluster_returnsNotFound(String quorum) {
    Response response =
        request("/v3/clusters/foobar/topics/" + TOPIC_NAME + "/partitions/0")
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }
}
