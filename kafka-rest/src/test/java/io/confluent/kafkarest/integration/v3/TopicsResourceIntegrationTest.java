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

import io.confluent.kafkarest.Versions;
import io.confluent.kafkarest.entities.ConfigSource;
import io.confluent.kafkarest.entities.v3.CollectionLink;
import io.confluent.kafkarest.entities.v3.ConfigSynonymData;
import io.confluent.kafkarest.entities.v3.CreateTopicResponse;
import io.confluent.kafkarest.entities.v3.GetTopicConfigResponse;
import io.confluent.kafkarest.entities.v3.GetTopicResponse;
import io.confluent.kafkarest.entities.v3.ListTopicsResponse;
import io.confluent.kafkarest.entities.v3.Relationship;
import io.confluent.kafkarest.entities.v3.ResourceLink;
import io.confluent.kafkarest.entities.v3.TopicConfigData;
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
  public void listTopics_existingCluster_returnsTopics() {
    String baseUrl = restConnect;
    String clusterId = getClusterId();

    ListTopicsResponse expected =
            new ListTopicsResponse(
                new CollectionLink(
                    baseUrl + "/v3/clusters/" + clusterId + "/topics", /* next= */ null),
                Arrays.asList(
                    new TopicData(
                        "crn:///kafka=" + clusterId + "/topic=" + TOPIC_1,
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
                                + "/configs"),
                        new Relationship(
                            baseUrl
                                + "/v3/clusters/" + clusterId
                                + "/topics/" + TOPIC_1
                                + "/partitions")),
                    new TopicData(
                        "crn:///kafka=" + clusterId + "/topic=" + TOPIC_2,
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
                                + "/configs"),
                        new Relationship(
                            baseUrl
                                + "/v3/clusters/" + clusterId
                                + "/topics/" + TOPIC_2
                                + "/partitions")),
                    new TopicData(
                        "crn:///kafka=" + clusterId + "/topic=" + TOPIC_3,
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
                                + "/configs"),
                        new Relationship(
                            baseUrl
                                + "/v3/clusters/" + clusterId
                                + "/topics/" + TOPIC_3
                                + "/partitions"))));

    Response response =
        request("/v3/clusters/" + clusterId + "/topics").accept(Versions.JSON_API).get();
    assertEquals(Status.OK.getStatusCode(), response.getStatus());

    ListTopicsResponse actual =
            response.readEntity(ListTopicsResponse.class);
    assertEquals(expected, actual);
  }

  @Test
  public void listTopics_nonExistingCluster_returnsNotFound() {
    Response response = request("/v3/clusters/foobar/topics").accept(Versions.JSON_API).get();
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @Test
  public void getTopic_existingClusterExistingTopic_returnsTopic() {
    String baseUrl = restConnect;
    String clusterId = getClusterId();

    GetTopicResponse expected =
            new GetTopicResponse(
                new TopicData(
                    "crn:///kafka=" + clusterId + "/topic=" + TOPIC_1,
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
                            + "/configs"),
                    new Relationship(
                        baseUrl
                            + "/v3/clusters/" + clusterId
                            + "/topics/" + TOPIC_1
                            + "/partitions")));

    Response response =
        request("/v3/clusters/" + clusterId + "/topics/" + TOPIC_1).accept(Versions.JSON_API).get();
    assertEquals(Status.OK.getStatusCode(), response.getStatus());

    GetTopicResponse actual =
            response.readEntity(GetTopicResponse.class);
    assertEquals(expected, actual);
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
  public void createTopic_nonExistingTopic_returnsCreatedTopic() {
    String baseUrl = restConnect;
    String clusterId = getClusterId();
    String topicName = "topic-4";

    CreateTopicResponse expected =
            new CreateTopicResponse(
                new TopicData(
                    "crn:///kafka=" + clusterId + "/topic=" + topicName,
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
                            + "/configs"),
                    new Relationship(
                        baseUrl
                            + "/v3/clusters/" + clusterId
                            + "/topics/" + topicName
                            + "/partitions")));

    Response response =
        request("/v3/clusters/" + clusterId + "/topics")
            .accept(Versions.JSON_API)
            .post(
                Entity.entity(
                    "{\"data\":{\"attributes\":{\"topic_name\":\""
                        + topicName + "\",\"partitions_count\":1,\"replication_factor\":1}}}",
                    Versions.JSON_API));
    assertEquals(Status.CREATED.getStatusCode(), response.getStatus());

    CreateTopicResponse actual =
            response.readEntity(CreateTopicResponse.class);
    assertEquals(expected, actual);

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
  public void deleteTopic_nonExistingCluster_noContentType_returnsNotFound() {
    Response response = request("/v3/clusters/foobar/topics/" + TOPIC_1).delete();
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @Test
  public void createAndDelete_nonExisting_returnsNotFoundCreatedAndNotFound() {
    String baseUrl = restConnect;
    String clusterId = getClusterId();
    String topicName = "topic-4";

    Response nonExistingGetTopicResponse =
        request("/v3/clusters/" + clusterId + "/topics/" + topicName)
            .accept(Versions.JSON_API)
            .get();
    assertEquals(Status.NOT_FOUND.getStatusCode(), nonExistingGetTopicResponse.getStatus());

    CreateTopicResponse expectedCreateTopicResponse =
            new CreateTopicResponse(
                new TopicData(
                    "crn:///kafka=" + clusterId + "/topic=" + topicName,
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
                            + "/configs"),
                    new Relationship(
                        baseUrl
                            + "/v3/clusters/" + clusterId
                            + "/topics/" + topicName
                            + "/partitions")));

    Response createTopicResponse =
        request("/v3/clusters/" + clusterId + "/topics")
            .accept(Versions.JSON_API)
            .post(
                Entity.entity(
                    "{\"data\":{\"attributes\":{\"topic_name\":\""
                        + topicName + "\",\"partitions_count\":1,\"replication_factor\":1,"
                        + "\"configs\":[{\"name\":\"cleanup.policy\",\"value\":\"compact\"}]}}}",
                    Versions.JSON_API));
    assertEquals(Status.CREATED.getStatusCode(), createTopicResponse.getStatus());

    CreateTopicResponse actualCreateTopicResponse =
            createTopicResponse.readEntity(CreateTopicResponse.class);

    assertEquals(expectedCreateTopicResponse, actualCreateTopicResponse);
    assertTrue(getTopicNames().contains(topicName));

    GetTopicResponse expectedExistingGetTopicResponse =
            new GetTopicResponse(
                new TopicData(
                    "crn:///kafka=" + clusterId + "/topic=" + topicName,
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
                            + "/configs"),
                    new Relationship(
                        baseUrl
                            + "/v3/clusters/" + clusterId
                            + "/topics/" + topicName
                            + "/partitions")));

    Response existingTopicResponse =
        request("/v3/clusters/" + clusterId + "/topics/" + topicName)
            .accept(Versions.JSON_API)
            .get();
    assertEquals(Status.OK.getStatusCode(), existingTopicResponse.getStatus());

    GetTopicResponse actualExistingGetTopicResponse =
            existingTopicResponse.readEntity(GetTopicResponse.class);
    assertEquals(expectedExistingGetTopicResponse, actualExistingGetTopicResponse);

    GetTopicConfigResponse expectedExistingGetTopicConfigResponse =
            new GetTopicConfigResponse(
                new TopicConfigData(
                    "crn:///kafka=" + clusterId
                        + "/topic=" + topicName
                        + "/config=cleanup.policy",
                    new ResourceLink(
                        baseUrl
                            + "/v3/clusters/" + clusterId
                            + "/topics/" + topicName
                            + "/configs/cleanup.policy"),
                    clusterId,
                    topicName,
                    "cleanup.policy",
                    "compact",
                    /* isDefault= */ false,
                    /* isReadOnly= */ false,
                    /* isSensitive= */ false,
                    ConfigSource.DYNAMIC_TOPIC_CONFIG,
                    Arrays.asList(
                        new ConfigSynonymData(
                            "cleanup.policy", "compact", ConfigSource.DYNAMIC_TOPIC_CONFIG),
                        new ConfigSynonymData(
                            "log.cleanup.policy", "delete", ConfigSource.DEFAULT_CONFIG))));

    Response existingGetTopicConfigResponse =
        request(
            "/v3/clusters/" + clusterId
                + "/topics/" + topicName
                + "/configs/cleanup.policy")
            .accept(Versions.JSON_API)
            .get();
    assertEquals(Status.OK.getStatusCode(), existingGetTopicConfigResponse.getStatus());

    GetTopicConfigResponse actualGetTopicConfigResponse =
            existingGetTopicConfigResponse.readEntity(GetTopicConfigResponse.class);
    assertEquals(
        expectedExistingGetTopicConfigResponse,
        actualGetTopicConfigResponse);

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
