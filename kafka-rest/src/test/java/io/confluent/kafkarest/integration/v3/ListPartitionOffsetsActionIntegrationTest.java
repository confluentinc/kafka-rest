/*
 * Copyright 2025 Confluent Inc.
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
import static org.junit.jupiter.api.Assertions.assertEquals;

import io.confluent.kafkarest.entities.v3.ListPartitionOffsetsResponse;
import io.confluent.kafkarest.entities.v3.PartitionWithOffsetsData;
import io.confluent.kafkarest.entities.v3.Resource;
import io.confluent.kafkarest.integration.ClusterTestHarness;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class ListPartitionOffsetsActionIntegrationTest extends ClusterTestHarness {

  private static final String TOPIC_NAME = "topic-1";

  private static final int PARTITION_ID = 0;

  public ListPartitionOffsetsActionIntegrationTest() {
    super(/* numBrokers= */ 1, /* withSchemaRegistry= */ false);
  }

  @BeforeEach
  @Override
  public void setUp(TestInfo testInfo) throws Exception {
    super.setUp(testInfo);

    createTopic(TOPIC_NAME, 1, (short) 1);
  }

  @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
  @ValueSource(strings = {"kraft"})
  public void listPartitionOffsets_existingPartitionWithOffsets_returnsPartitionWithOffsets(
      String quorum) throws Exception {
    String baseUrl = restConnect;
    String clusterId = getClusterId();

    ListPartitionOffsetsResponse expected =
        ListPartitionOffsetsResponse.create(
            PartitionWithOffsetsData.builder()
                .setMetadata(
                    Resource.Metadata.builder()
                        .setSelf(
                            baseUrl
                                + "/v3/clusters/"
                                + clusterId
                                + "/topics/"
                                + TOPIC_NAME
                                + "/partitions/"
                                + String.valueOf(PARTITION_ID)
                                + "/offset")
                        .setResourceName(
                            "crn:///kafka="
                                + clusterId
                                + "/topic="
                                + TOPIC_NAME
                                + "/partition="
                                + String.valueOf(PARTITION_ID)
                                + "/offset")
                        .build())
                .setClusterId(clusterId)
                .setTopicName(TOPIC_NAME)
                .setPartitionId(PARTITION_ID)
                .setEarliestOffset(0L)
                .setLatestOffset(0L)
                .build());

    Response response =
        request(
                "/v3/clusters/"
                    + clusterId
                    + "/topics/"
                    + TOPIC_NAME
                    + "/partitions/"
                    + String.valueOf(PARTITION_ID)
                    + "/offset")
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(Status.OK.getStatusCode(), response.getStatus());

    ListPartitionOffsetsResponse actual = response.readEntity(ListPartitionOffsetsResponse.class);
    assertEquals(expected, actual);
  }

  @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
  @ValueSource(strings = {"kraft"})
  public void listPartitionOffsets_nonExistingPartition_returnsNotFound(String quorum) {
    String clusterId = getClusterId();

    Response response =
        request("/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME + "/partitions/100/offset")
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
  @ValueSource(strings = {"kraft"})
  public void getPartition_nonExistingTopic_returnsNotFound(String quorum) {
    String clusterId = getClusterId();

    Response response =
        request("/v3/clusters/" + clusterId + "/topics/foobar/partitions/0/offset")
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
  @ValueSource(strings = {"kraft"})
  public void getPartition_nonExistingCluster_returnsNotFound(String quorum) {
    Response response =
        request("/v3/clusters/foobar/topics/" + TOPIC_NAME + "/partitions/0/offset")
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }
}
