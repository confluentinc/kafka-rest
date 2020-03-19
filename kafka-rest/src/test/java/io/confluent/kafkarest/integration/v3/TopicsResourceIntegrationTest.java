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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafkarest.Versions;
import io.confluent.kafkarest.entities.v3.CollectionLink;
import io.confluent.kafkarest.entities.v3.CreateTopicResponse;
import io.confluent.kafkarest.entities.v3.GetTopicConfigurationResponse;
import io.confluent.kafkarest.entities.v3.GetTopicResponse;
import io.confluent.kafkarest.entities.v3.ListTopicsResponse;
import io.confluent.kafkarest.entities.v3.Relationship;
import io.confluent.kafkarest.entities.v3.ResourceLink;
import io.confluent.kafkarest.entities.v3.TopicConfigurationData;
import io.confluent.kafkarest.entities.v3.TopicData;
import io.confluent.kafkarest.integration.ClusterTestHarness;
import java.util.Arrays;
import java.util.Properties;
import javax.ws.rs.client.Entity;
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

  @Override
  public Properties overrideBrokerProperties(int i, Properties props) {
    props.put("delete.topic.enable", true);
    return props;
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
                        "crn://kafka=" + clusterId + "/topic=" + TOPIC_1,
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
                        "crn://kafka=" + clusterId + "/topic=" + TOPIC_2,
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
                        "crn://kafka=" + clusterId + "/topic=" + TOPIC_3,
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
                    "crn://kafka=" + clusterId + "/topic=" + TOPIC_1,
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

  @Test
  public void createTopic_nonExistingTopic_returnsCreatedTopic() throws Exception {
    String baseUrl = restConnect;
    String clusterId = getClusterId();
    String topicName = "topic-4";

    String expected =
        OBJECT_MAPPER.writeValueAsString(
            new CreateTopicResponse(
                new TopicData(
                    "crn://kafka=" + clusterId + "/topic=" + topicName,
                    new ResourceLink(
                        baseUrl + "/v3/clusters/" + clusterId + "/topics/" + topicName),
                    clusterId,
                    topicName,
                    /* isInternal= */ false,
                    /* replicationFactor= */ 1,
                    new Relationship(
                        baseUrl
                            + "/v3/clusters/" + clusterId
                            + "/topics/" + topicName
                            + "/configurations"),
                    new Relationship(
                        baseUrl
                            + "/v3/clusters/" + clusterId
                            + "/topics/" + topicName
                            + "/partitions"))));

    Response response =
        request("/v3/clusters/" + clusterId + "/topics")
            .accept(Versions.JSON_API)
            .post(
                Entity.entity(
                    "{\"data\":{\"attributes\":{\"topic_name\":\""
                        + topicName + "\",\"partitions_count\":1,\"replication_factor\":1}}}",
                    Versions.JSON_API));
    assertEquals(Status.CREATED.getStatusCode(), response.getStatus());
    assertEquals(expected, response.readEntity(String.class));
    assertTrue(getTopicNames().contains(topicName));
  }

  @Test
  public void createTopic_existingTopic_returnsBadRequest() {
    String clusterId = getClusterId();

    Response response =
        request("/v3/clusters/" + clusterId + "/topics")
            .accept(Versions.JSON_API)
            .post(
                Entity.entity(
                    "{\"data\":{\"attributes\":{\"topic_name\":\""
                        + TOPIC_1 + "\",\"partitions_count\":1,\"replication_factor\":1}}}",
                    Versions.JSON_API));
    assertEquals(Status.BAD_REQUEST.getStatusCode(), response.getStatus());
  }

  @Test
  public void createTopic_nonExistingCluster_returnsNotFound() {
    Response response =
        request("/v3/clusters/foobar/topics")
            .accept(Versions.JSON_API)
            .post(
                Entity.entity(
                    "{\"data\":{\"attributes\":{\"topic_name\":\"topic-4\",\"partitions_count\":1,"
                        + "\"replication_factor\":1}}}",
                    Versions.JSON_API));
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @Test
  public void deleteTopic_existingTopic_deletesTopic() {
    String clusterId = getClusterId();

    Response response =
        request("/v3/clusters/" + clusterId + "/topics/" + TOPIC_1)
            .accept(Versions.JSON_API)
            .delete();
    assertEquals(Status.NO_CONTENT.getStatusCode(), response.getStatus());
    assertTrue(response.readEntity(String.class).isEmpty());
    assertFalse(getTopicNames().contains(TOPIC_1));
  }

  @Test
  public void deleteTopic_nonExistingTopic_returnsNotFound() {
    String clusterId = getClusterId();

    Response response =
        request("/v3/clusters/" + clusterId + "/topics/foobar")
            .accept(Versions.JSON_API)
            .delete();
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @Test
  public void deleteTopic_nonExistingCluster_returnsNotFound() {
    Response response =
        request("/v3/clusters/foobar/topics/" + TOPIC_1)
            .accept(Versions.JSON_API)
            .delete();
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @Test
  public void
  getTopic_nonExistingTopic_returnsEmpty_thenCreateTopicAndGetTopic_returnsCreatedTopic_thenDeleteTopicAndGetTopic_returnsEmpty
      () throws Exception {
    String baseUrl = restConnect;
    String clusterId = getClusterId();
    String topicName = "topic-4";

    Response nonExistingGetTopicResponse =
        request("/v3/clusters/" + clusterId + "/topics/" + topicName)
            .accept(Versions.JSON_API)
            .get();
    assertEquals(Status.NOT_FOUND.getStatusCode(), nonExistingGetTopicResponse.getStatus());

    String expectedCreateTopicResponse =
        OBJECT_MAPPER.writeValueAsString(
            new CreateTopicResponse(
                new TopicData(
                    "crn://kafka=" + clusterId + "/topic=" + topicName,
                    new ResourceLink(
                        baseUrl + "/v3/clusters/" + clusterId + "/topics/" + topicName),
                    clusterId,
                    topicName,
                    /* isInternal= */ false,
                    /* replicationFactor= */ 1,
                    new Relationship(
                        baseUrl
                            + "/v3/clusters/" + clusterId
                            + "/topics/" + topicName
                            + "/configurations"),
                    new Relationship(
                        baseUrl
                            + "/v3/clusters/" + clusterId
                            + "/topics/" + topicName
                            + "/partitions"))));

    Response createTopicResponse =
        request("/v3/clusters/" + clusterId + "/topics")
            .accept(Versions.JSON_API)
            .post(
                Entity.entity(
                    "{\"data\":{\"attributes\":{\"topic_name\":\""
                        + topicName + "\",\"partitions_count\":1,\"replication_factor\":1,"
                        + "\"configurations\":[{\"name\":\"cleanup.policy\",\"value\":\"compact\"}]"
                        + "}}}",
                    Versions.JSON_API));
    assertEquals(Status.CREATED.getStatusCode(), createTopicResponse.getStatus());
    assertEquals(expectedCreateTopicResponse, createTopicResponse.readEntity(String.class));
    assertTrue(getTopicNames().contains(topicName));

    String expectedExistingGetTopicResponse =
        OBJECT_MAPPER.writeValueAsString(
            new GetTopicResponse(
                new TopicData(
                    "crn://kafka=" + clusterId + "/topic=" + topicName,
                    new ResourceLink(
                        baseUrl
                            + "/v3/clusters/" + clusterId
                            + "/topics/" + topicName),
                    clusterId,
                    topicName,
                    /* isInternal= */ false,
                    /* replicationFactor= */ 1,
                    new Relationship(
                        baseUrl
                            + "/v3/clusters/" + clusterId
                            + "/topics/" + topicName
                            + "/configurations"),
                    new Relationship(
                        baseUrl
                            + "/v3/clusters/" + clusterId
                            + "/topics/" + topicName
                            + "/partitions"))));

    Response existingTopicResponse =
        request("/v3/clusters/" + clusterId + "/topics/" + topicName)
            .accept(Versions.JSON_API)
            .get();
    assertEquals(Status.OK.getStatusCode(), existingTopicResponse.getStatus());
    assertEquals(expectedExistingGetTopicResponse, existingTopicResponse.readEntity(String.class));

    String expectedExistingGetTopicConfigurationResponse =
        OBJECT_MAPPER.writeValueAsString(
            new GetTopicConfigurationResponse(
                new TopicConfigurationData(
                    "crn://kafka=" + clusterId
                        + "/topic=" + topicName
                        + "/configuration=cleanup.policy",
                    new ResourceLink(
                        baseUrl
                            + "/v3/clusters/" + clusterId
                            + "/topics/" + topicName
                            + "/configurations/cleanup.policy"),
                    clusterId,
                    topicName,
                    "cleanup.policy",
                    "compact",
                    /* isDefault= */ false,
                    /* isReadOnly= */ false,
                    /* isSensitive= */ false)));

    Response existingGetTopicConfigurationResponse =
        request(
            "/v3/clusters/" + clusterId
                + "/topics/" + topicName
                + "/configurations/cleanup.policy")
            .accept(Versions.JSON_API)
            .get();
    assertEquals(Status.OK.getStatusCode(), existingGetTopicConfigurationResponse.getStatus());
    assertEquals(
        expectedExistingGetTopicConfigurationResponse,
        existingGetTopicConfigurationResponse.readEntity(String.class));

    Response deleteTopicResponse =
        request("/v3/clusters/" + clusterId + "/topics/" + topicName)
            .accept(Versions.JSON_API)
            .delete();
    assertEquals(Status.NO_CONTENT.getStatusCode(), deleteTopicResponse.getStatus());
    assertTrue(deleteTopicResponse.readEntity(String.class).isEmpty());
    assertFalse(getTopicNames().contains(topicName));

    Response deletedGetTopicResponse =
        request("/v3/clusters/" + clusterId + "/topics/" + topicName)
            .accept(Versions.JSON_API)
            .get();
    assertEquals(Status.NOT_FOUND.getStatusCode(), deletedGetTopicResponse.getStatus());
  }
}
