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

import io.confluent.kafkarest.entities.v3.ReplicaData;
import io.confluent.kafkarest.entities.v3.ReplicaDataList;
import io.confluent.kafkarest.entities.v3.Resource;
import io.confluent.kafkarest.entities.v3.ResourceCollection;
import io.confluent.kafkarest.entities.v3.SearchReplicasByBrokerResponse;
import io.confluent.kafkarest.integration.ClusterTestHarness;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import org.junit.Before;
import org.junit.Test;

public class SearchReplicasByBrokerActionIntegrationTest extends ClusterTestHarness {

  private static final int BROKER_ID = 0;
  private static final String TOPIC_NAME = "topic-1";

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
  public void searchReplicasByBroker_existingBroker_returnsReplicas() {
    String baseUrl = restConnect;
    String clusterId = getClusterId();

    SearchReplicasByBrokerResponse expected =
        SearchReplicasByBrokerResponse.create(
            ReplicaDataList.builder()
                .setMetadata(
                    ResourceCollection.Metadata.builder()
                        .setSelf(
                            baseUrl
                                + "/v3/clusters/" + clusterId
                                + "/brokers/" + BROKER_ID
                                + "/partition-replicas")
                        .build())
                .setData(
                    Arrays.asList(
                        ReplicaData.builder()
                            .setMetadata(
                                Resource.Metadata.builder()
                                    .setSelf(
                                        baseUrl
                                            + "/v3/clusters/" + clusterId
                                            + "/topics/" + TOPIC_NAME
                                            + "/partitions/0/replicas/" + BROKER_ID)
                                    .setResourceName(
                                        "crn:///kafka=" + clusterId
                                            + "/topic=" + TOPIC_NAME
                                            + "/partition=0/replica=" + BROKER_ID)
                                    .build())
                            .setClusterId(clusterId)
                            .setTopicName(TOPIC_NAME)
                            .setPartitionId(0)
                            .setBrokerId(BROKER_ID)
                            .setLeader(true)
                            .setInSync(true)
                            .setBroker(
                                Resource.Relationship.create(
                                    baseUrl
                                        + "/v3/clusters/" + clusterId
                                        + "/brokers/" + BROKER_ID))
                            .build(),
                        ReplicaData.builder()
                            .setMetadata(
                                Resource.Metadata.builder()
                                    .setSelf(
                                        baseUrl
                                            + "/v3/clusters/" + clusterId
                                            + "/topics/" + TOPIC_NAME
                                            + "/partitions/1/replicas/" + BROKER_ID)
                                    .setResourceName(
                                        "crn:///kafka=" + clusterId
                                            + "/topic=" + TOPIC_NAME
                                            + "/partition=1/replica=" + BROKER_ID)
                                    .build())
                            .setClusterId(clusterId)
                            .setTopicName(TOPIC_NAME)

                            .setPartitionId(1)
                            .setBrokerId(BROKER_ID)
                            .setLeader(false)
                            .setInSync(true)
                            .setBroker(
                                Resource.Relationship.create(
                                    baseUrl
                                        + "/v3/clusters/" + clusterId
                                        + "/brokers/" + BROKER_ID))
                            .build()))
                .build());

    Response response =
        request("/v3/clusters/" + clusterId + "/brokers/" + BROKER_ID + "/partition-replicas")
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(Status.OK.getStatusCode(), response.getStatus());

    SearchReplicasByBrokerResponse actual =
        response.readEntity(SearchReplicasByBrokerResponse.class);
    assertEquals(expected, actual);
  }

  @Test
  public void searchReplicasByBroker_nonExistingBroker_returnsNotFound() {
    String clusterId = getClusterId();

    Response response =
        request("/v3/clusters/" + clusterId + "/brokers/100/partition-replicas")
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }
}
