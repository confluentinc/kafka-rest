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
import io.confluent.kafkarest.entities.v3.CollectionLink;
import io.confluent.kafkarest.entities.v3.GetTopicResponse;
import io.confluent.kafkarest.entities.v3.ListTopicsResponse;
import io.confluent.kafkarest.entities.v3.Relationship;
import io.confluent.kafkarest.entities.v3.ResourceLink;
import io.confluent.kafkarest.entities.v3.TopicData;
import io.confluent.kafkarest.integration.ClusterTestHarness;
import java.util.Arrays;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import org.junit.Before;
import org.junit.Test;

public class TopicsResourceIntegrationTest extends ClusterTestHarness {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private static final String TOPIC_1 = "topic-1";
  private static final String TOPIC_2 = "topic-2";
  private static final String TOPIC_3 = "topic-3";

  public TopicsResourceIntegrationTest() {
    super(/* numBrokers= */ 1, /* withSchemaRegistry= */ false);
  }

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();

    createTopic(TOPIC_1, 1, (short) 1);
    createTopic(TOPIC_2, 1, (short) 1);
    createTopic(TOPIC_3, 1, (short) 1);
  }

  @Test
  public void listTopics_existingCluster_returnsTopics() throws Exception {
    String baseUrl = restConnect;
    String clusterId = getClusterId();

    String expected =
        OBJECT_MAPPER.writeValueAsString(
            new ListTopicsResponse(
                new CollectionLink(
                    baseUrl + "/v3/clusters/" + clusterId + "/topics", /* next= */ null),
                Arrays.asList(
                    new TopicData(
                        new ResourceLink(
                            baseUrl
                                + "/v3/clusters/" + clusterId
                                + "/topics/" + TOPIC_1),
                        clusterId,
                        TOPIC_1,
                        /* isInternal= */ false,
                        /* replicationFactor= */ 1,
                        new Relationship(
                            baseUrl
                                + "/v3/clusters/" + clusterId
                                + "/topics/" + TOPIC_1
                                + "/configurations"),
                        new Relationship(
                            baseUrl
                                + "/v3/clusters/" + clusterId
                                + "/topics/" + TOPIC_1
                                + "/partitions")),
                    new TopicData(
                        new ResourceLink(
                            baseUrl
                                + "/v3/clusters/" + clusterId
                                + "/topics/" + TOPIC_2),
                        clusterId,
                        TOPIC_2,
                        /* isInternal= */ false,
                        /* replicationFactor= */ 1,
                        new Relationship(
                            baseUrl
                                + "/v3/clusters/" + clusterId
                                + "/topics/" + TOPIC_2
                                + "/configurations"),
                        new Relationship(
                            baseUrl
                                + "/v3/clusters/" + clusterId
                                + "/topics/" + TOPIC_2
                                + "/partitions")),
                    new TopicData(
                        new ResourceLink(
                            baseUrl
                                + "/v3/clusters/" + clusterId
                                + "/topics/" + TOPIC_3),
                        clusterId,
                        TOPIC_3,
                        /* isInternal= */ false,
                        /* replicationFactor= */ 1,
                        new Relationship(
                            baseUrl
                                + "/v3/clusters/" + clusterId
                                + "/topics/" + TOPIC_3
                                + "/configurations"),
                        new Relationship(
                            baseUrl
                                + "/v3/clusters/" + clusterId
                                + "/topics/" + TOPIC_3
                                + "/partitions")))));

    Response response =
        request("/v3/clusters/" + clusterId + "/topics").accept(Versions.JSON_API).get();
    assertEquals(Status.OK.getStatusCode(), response.getStatus());
    assertEquals(expected, response.readEntity(String.class));
  }

  @Test
  public void listTopics_nonExistingCluster_returnsNotFound() {
    Response response = request("/v3/clusters/foobar/topics").accept(Versions.JSON_API).get();
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @Test
  public void getTopic_existingClusterExistingTopic_returnsTopic() throws Exception {
    String baseUrl = restConnect;
    String clusterId = getClusterId();

    String expected =
        OBJECT_MAPPER.writeValueAsString(
            new GetTopicResponse(
                new TopicData(
                    new ResourceLink(
                        baseUrl
                            + "/v3/clusters/" + clusterId
                            + "/topics/" + TOPIC_1),
                    clusterId,
                    TOPIC_1,
                    /* isInternal= */ false,
                    /* replicationFactor= */ 1,
                    new Relationship(
                        baseUrl
                            + "/v3/clusters/" + clusterId
                            + "/topics/" + TOPIC_1
                            + "/configurations"),
                    new Relationship(
                        baseUrl
                            + "/v3/clusters/" + clusterId
                            + "/topics/" + TOPIC_1
                            + "/partitions"))));

    Response response =
        request("/v3/clusters/" + clusterId + "/topics/" + TOPIC_1).accept(Versions.JSON_API).get();
    assertEquals(Status.OK.getStatusCode(), response.getStatus());
    assertEquals(expected, response.readEntity(String.class));
  }

  @Test
  public void getTopic_nonExistingCluster_returnsNotFound() {
    Response response =
        request("/v3/clusters/foobar/topics/" + TOPIC_1).accept(Versions.JSON_API).get();
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @Test
  public void getTopic_nonExistingTopic_returnsNotFound() {
    String clusterId = getClusterId();

    Response response =
        request("/v3/clusters/" + clusterId + "/topics/foobar").accept(Versions.JSON_API).get();
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }
}
