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
import static io.confluent.kafkarest.TestUtils.testWithRetry;
import static java.util.Collections.emptySet;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.confluent.kafkarest.entities.ConfigSource;
import io.confluent.kafkarest.entities.v3.ConfigSynonymData;
import io.confluent.kafkarest.entities.v3.CreateTopicResponse;
import io.confluent.kafkarest.entities.v3.GetTopicConfigResponse;
import io.confluent.kafkarest.entities.v3.GetTopicResponse;
import io.confluent.kafkarest.entities.v3.ListTopicsResponse;
import io.confluent.kafkarest.entities.v3.Resource;
import io.confluent.kafkarest.entities.v3.ResourceCollection;
import io.confluent.kafkarest.entities.v3.TopicConfigData;
import io.confluent.kafkarest.entities.v3.TopicData;
import io.confluent.kafkarest.entities.v3.TopicDataList;
import io.confluent.kafkarest.integration.ClusterTestHarness;
import java.util.Arrays;
import java.util.Properties;
import javax.ws.rs.HttpMethod;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class TopicsResourceIntegrationTest extends ClusterTestHarness {

  private static final String TOPIC_1 = "topic-1";
  private static final String TOPIC_2 = "topic-2";
  private static final String TOPIC_3 = "topic-3";
  private static final String TOPIC_NON_EXISTENT = "no-such-topic";

  private static final boolean USE_ALTERNATE_CLIENT_PROVIDER = true;

  public TopicsResourceIntegrationTest() {
    super(/* numBrokers= */ 3, /* withSchemaRegistry= */ false);
  }

  @BeforeEach
  @Override
  public void setUp(TestInfo testInfo) throws Exception {
    super.setUp(testInfo);

    createTopic(TOPIC_1, 2, (short) 1);
    createTopic(TOPIC_2, 1, (short) 1);
    createTopic(TOPIC_3, 1, (short) 1);
  }

  @Override
  protected Properties overrideKraftControllerConfig() {
    Properties props = new Properties();
    props.put("delete.topic.enable", true);
    props.put("default.replication.factor", 2);
    return props;
  }

  @Override
  public Properties overrideBrokerProperties(int i, Properties props) {
    props.put("delete.topic.enable", true);
    props.put("default.replication.factor", 2);
    return props;
  }

  @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
  @ValueSource(strings = {"kraft", "zk"})
  public void listTopics_existingCluster_returnsTopics(String quorum) {
    String baseUrl = restConnect;
    String clusterId = getClusterId();

    ListTopicsResponse expected =
        ListTopicsResponse.create(
            TopicDataList.builder()
                .setMetadata(
                    ResourceCollection.Metadata.builder()
                        .setSelf(baseUrl + "/v3/clusters/" + clusterId + "/topics")
                        .build())
                .setData(
                    Arrays.asList(
                        TopicData.builder()
                            .setMetadata(
                                Resource.Metadata.builder()
                                    .setSelf(
                                        baseUrl
                                            + "/v3/clusters/"
                                            + clusterId
                                            + "/topics/"
                                            + TOPIC_1)
                                    .setResourceName(
                                        "crn:///kafka=" + clusterId + "/topic=" + TOPIC_1)
                                    .build())
                            .setClusterId(clusterId)
                            .setTopicName(TOPIC_1)
                            .setInternal(false)
                            .setReplicationFactor(1)
                            .setPartitionsCount(2)
                            .setPartitions(
                                Resource.Relationship.create(
                                    baseUrl
                                        + "/v3/clusters/"
                                        + clusterId
                                        + "/topics/"
                                        + TOPIC_1
                                        + "/partitions"))
                            .setConfigs(
                                Resource.Relationship.create(
                                    baseUrl
                                        + "/v3/clusters/"
                                        + clusterId
                                        + "/topics/"
                                        + TOPIC_1
                                        + "/configs"))
                            .setPartitionReassignments(
                                Resource.Relationship.create(
                                    baseUrl
                                        + "/v3/clusters/"
                                        + clusterId
                                        + "/topics/"
                                        + TOPIC_1
                                        + "/partitions/-/reassignment"))
                            .setAuthorizedOperations(emptySet())
                            .build(),
                        TopicData.builder()
                            .setMetadata(
                                Resource.Metadata.builder()
                                    .setSelf(
                                        baseUrl
                                            + "/v3/clusters/"
                                            + clusterId
                                            + "/topics/"
                                            + TOPIC_2)
                                    .setResourceName(
                                        "crn:///kafka=" + clusterId + "/topic=" + TOPIC_2)
                                    .build())
                            .setClusterId(clusterId)
                            .setTopicName(TOPIC_2)
                            .setInternal(false)
                            .setReplicationFactor(1)
                            .setPartitionsCount(1)
                            .setPartitions(
                                Resource.Relationship.create(
                                    baseUrl
                                        + "/v3/clusters/"
                                        + clusterId
                                        + "/topics/"
                                        + TOPIC_2
                                        + "/partitions"))
                            .setConfigs(
                                Resource.Relationship.create(
                                    baseUrl
                                        + "/v3/clusters/"
                                        + clusterId
                                        + "/topics/"
                                        + TOPIC_2
                                        + "/configs"))
                            .setPartitionReassignments(
                                Resource.Relationship.create(
                                    baseUrl
                                        + "/v3/clusters/"
                                        + clusterId
                                        + "/topics/"
                                        + TOPIC_2
                                        + "/partitions/-/reassignment"))
                            .setAuthorizedOperations(emptySet())
                            .build(),
                        TopicData.builder()
                            .setMetadata(
                                Resource.Metadata.builder()
                                    .setSelf(
                                        baseUrl
                                            + "/v3/clusters/"
                                            + clusterId
                                            + "/topics/"
                                            + TOPIC_3)
                                    .setResourceName(
                                        "crn:///kafka=" + clusterId + "/topic=" + TOPIC_3)
                                    .build())
                            .setClusterId(clusterId)
                            .setTopicName(TOPIC_3)
                            .setInternal(false)
                            .setReplicationFactor(1)
                            .setPartitionsCount(1)
                            .setPartitions(
                                Resource.Relationship.create(
                                    baseUrl
                                        + "/v3/clusters/"
                                        + clusterId
                                        + "/topics/"
                                        + TOPIC_3
                                        + "/partitions"))
                            .setConfigs(
                                Resource.Relationship.create(
                                    baseUrl
                                        + "/v3/clusters/"
                                        + clusterId
                                        + "/topics/"
                                        + TOPIC_3
                                        + "/configs"))
                            .setPartitionReassignments(
                                Resource.Relationship.create(
                                    baseUrl
                                        + "/v3/clusters/"
                                        + clusterId
                                        + "/topics/"
                                        + TOPIC_3
                                        + "/partitions/-/reassignment"))
                            .setAuthorizedOperations(emptySet())
                            .build()))
                .build());

    testWithRetry(
        () -> {
          Response response =
              request("/v3/clusters/" + clusterId + "/topics")
                  .accept(MediaType.APPLICATION_JSON)
                  .get();

          assertEquals(Status.OK.getStatusCode(), response.getStatus());

          ListTopicsResponse actual = response.readEntity(ListTopicsResponse.class);
          assertEquals(expected, actual);
        });
  }

  @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
  @ValueSource(strings = {"kraft", "zk"})
  public void listTopics_nonExistingCluster_returnsNotFound(String quorum) {
    Response response =
        request("/v3/clusters/foobar/topics").accept(MediaType.APPLICATION_JSON).get();
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
  @ValueSource(strings = {"kraft", "zk"})
  public void getTopic_existingClusterExistingTopic_returnsTopic(String quorum) {
    String baseUrl = restConnect;
    String clusterId = getClusterId();

    GetTopicResponse expected =
        GetTopicResponse.create(
            TopicData.builder()
                .setMetadata(
                    Resource.Metadata.builder()
                        .setSelf(baseUrl + "/v3/clusters/" + clusterId + "/topics/" + TOPIC_1)
                        .setResourceName("crn:///kafka=" + clusterId + "/topic=" + TOPIC_1)
                        .build())
                .setClusterId(clusterId)
                .setTopicName(TOPIC_1)
                .setInternal(false)
                .setReplicationFactor(1)
                .setPartitionsCount(2)
                .setPartitions(
                    Resource.Relationship.create(
                        baseUrl
                            + "/v3/clusters/"
                            + clusterId
                            + "/topics/"
                            + TOPIC_1
                            + "/partitions"))
                .setConfigs(
                    Resource.Relationship.create(
                        baseUrl + "/v3/clusters/" + clusterId + "/topics/" + TOPIC_1 + "/configs"))
                .setPartitionReassignments(
                    Resource.Relationship.create(
                        baseUrl
                            + "/v3/clusters/"
                            + clusterId
                            + "/topics/"
                            + TOPIC_1
                            + "/partitions/-/reassignment"))
                .setAuthorizedOperations(emptySet())
                .build());

    testWithRetry(
        () -> {
          Response response =
              request("/v3/clusters/" + clusterId + "/topics/" + TOPIC_1)
                  .accept(MediaType.APPLICATION_JSON)
                  .get();
          assertEquals(Status.OK.getStatusCode(), response.getStatus());
          GetTopicResponse actual = response.readEntity(GetTopicResponse.class);
          assertEquals(expected, actual);
        });
  }

  @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
  @ValueSource(strings = {"kraft", "zk"})
  public void getTopic_nonExistingCluster_returnsNotFound(String quorum) {
    Response response =
        request("/v3/clusters/foobar/topics/" + TOPIC_1).accept(MediaType.APPLICATION_JSON).get();
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
  @ValueSource(strings = {"kraft", "zk"})
  public void getTopic_nonExistingTopic_returnsNotFound(String quorum) {
    String clusterId = getClusterId();

    Response response =
        request("/v3/clusters/" + clusterId + "/topics/foobar")
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
  @ValueSource(strings = {"kraft", "zk"})
  public void createTopic_nonExistingTopic_returnsCreatedTopic(String quorum) {
    String baseUrl = restConnect;
    String clusterId = getClusterId();
    String topicName = "topic-4";

    CreateTopicResponse expected =
        CreateTopicResponse.create(
            TopicData.builder()
                .setMetadata(
                    Resource.Metadata.builder()
                        .setSelf(baseUrl + "/v3/clusters/" + clusterId + "/topics/" + topicName)
                        .setResourceName("crn:///kafka=" + clusterId + "/topic=" + topicName)
                        .build())
                .setClusterId(clusterId)
                .setTopicName(topicName)
                .setInternal(false)
                .setReplicationFactor(1)
                .setPartitionsCount(1)
                .setPartitions(
                    Resource.Relationship.create(
                        baseUrl
                            + "/v3/clusters/"
                            + clusterId
                            + "/topics/"
                            + topicName
                            + "/partitions"))
                .setConfigs(
                    Resource.Relationship.create(
                        baseUrl
                            + "/v3/clusters/"
                            + clusterId
                            + "/topics/"
                            + topicName
                            + "/configs"))
                .setPartitionReassignments(
                    Resource.Relationship.create(
                        baseUrl
                            + "/v3/clusters/"
                            + clusterId
                            + "/topics/"
                            + topicName
                            + "/partitions/-/reassignment"))
                .setAuthorizedOperations(emptySet())
                .build());

    Response response =
        request("/v3/clusters/" + clusterId + "/topics")
            .accept(MediaType.APPLICATION_JSON)
            .post(
                Entity.entity(
                    "{\"topic_name\":\""
                        + topicName
                        + "\",\"partitions_count\":1,"
                        + "\"replication_factor\":1}",
                    MediaType.APPLICATION_JSON));
    assertEquals(Status.CREATED.getStatusCode(), response.getStatus());

    CreateTopicResponse actual = response.readEntity(CreateTopicResponse.class);
    assertEquals(expected, actual);

    testWithRetry(
        () ->
            assertTrue(
                getTopicNames().contains(topicName),
                String.format("Topic names should contain %s after its creation", topicName)));
  }

  @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
  @ValueSource(strings = {"kraft", "zk"})
  public void validateOnlyCreateTopic_nonExistingTopic_returnsNonCreatedTopic(String quorum) {
    String baseUrl = restConnect;
    String clusterId = getClusterId();
    String topicName = "topic-4";

    CreateTopicResponse expected =
        CreateTopicResponse.create(
            TopicData.builder()
                .setMetadata(
                    Resource.Metadata.builder()
                        .setSelf(baseUrl + "/v3/clusters/" + clusterId + "/topics/" + topicName)
                        .setResourceName("crn:///kafka=" + clusterId + "/topic=" + topicName)
                        .build())
                .setClusterId(clusterId)
                .setTopicName(topicName)
                .setInternal(false)
                .setReplicationFactor(1)
                .setPartitionsCount(1)
                .setPartitions(
                    Resource.Relationship.create(
                        baseUrl
                            + "/v3/clusters/"
                            + clusterId
                            + "/topics/"
                            + topicName
                            + "/partitions"))
                .setConfigs(
                    Resource.Relationship.create(
                        baseUrl
                            + "/v3/clusters/"
                            + clusterId
                            + "/topics/"
                            + topicName
                            + "/configs"))
                .setPartitionReassignments(
                    Resource.Relationship.create(
                        baseUrl
                            + "/v3/clusters/"
                            + clusterId
                            + "/topics/"
                            + topicName
                            + "/partitions/-/reassignment"))
                .setAuthorizedOperations(emptySet())
                .build());

    Response response =
        request("/v3/clusters/" + clusterId + "/topics")
            .accept(MediaType.APPLICATION_JSON)
            .post(
                Entity.entity(
                    "{\"topic_name\":\""
                        + topicName
                        + "\",\"partitions_count\":1,"
                        + "\"replication_factor\":1,"
                        + "\"validate_only\":true}",
                    MediaType.APPLICATION_JSON));
    assertEquals(Status.OK.getStatusCode(), response.getStatus());

    CreateTopicResponse actual = response.readEntity(CreateTopicResponse.class);
    assertEquals(expected, actual);

    testWithRetry(
        () ->
            assertFalse(
                getTopicNames().contains(topicName),
                String.format(
                    "Topic names should not contain %s after dry-run creation", topicName)));
  }

  @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
  @ValueSource(strings = {"kraft", "zk"})
  public void createTopic_nonExistingTopic_customReplicasAssignments_returnsCreatedTopic(
      String quorum) {
    String baseUrl = restConnect;
    String clusterId = getClusterId();
    String topicName = "topic-4";

    CreateTopicResponse expected =
        CreateTopicResponse.create(
            TopicData.builder()
                .setMetadata(
                    Resource.Metadata.builder()
                        .setSelf(baseUrl + "/v3/clusters/" + clusterId + "/topics/" + topicName)
                        .setResourceName("crn:///kafka=" + clusterId + "/topic=" + topicName)
                        .build())
                .setClusterId(clusterId)
                .setTopicName(topicName)
                .setInternal(false)
                .setReplicationFactor(2) // As determined by the actual replicas assignments below.
                .setPartitionsCount(3)
                .setPartitions(
                    Resource.Relationship.create(
                        baseUrl
                            + "/v3/clusters/"
                            + clusterId
                            + "/topics/"
                            + topicName
                            + "/partitions"))
                .setConfigs(
                    Resource.Relationship.create(
                        baseUrl
                            + "/v3/clusters/"
                            + clusterId
                            + "/topics/"
                            + topicName
                            + "/configs"))
                .setPartitionReassignments(
                    Resource.Relationship.create(
                        baseUrl
                            + "/v3/clusters/"
                            + clusterId
                            + "/topics/"
                            + topicName
                            + "/partitions/-/reassignment"))
                .setAuthorizedOperations(emptySet())
                .build());

    Response response =
        request("/v3/clusters/" + clusterId + "/topics")
            .accept(MediaType.APPLICATION_JSON)
            .post(
                Entity.entity(
                    "{\"topic_name\":\""
                        + topicName
                        + "\",\"replicas_assignments\":{\"0\":[0,1], \"1\":[1,2], \"2\":[2,0]}}",
                    MediaType.APPLICATION_JSON));
    assertEquals(Status.CREATED.getStatusCode(), response.getStatus());

    CreateTopicResponse actual = response.readEntity(CreateTopicResponse.class);
    assertEquals(expected, actual);

    testWithRetry(
        () ->
            assertTrue(
                getTopicNames().contains(topicName),
                String.format("Topic names should contain %s after its creation", topicName)));
  }

  @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
  @ValueSource(strings = {"kraft", "zk"})
  public void createTopic_existingTopic_returnsBadRequest(String quorum) {
    String clusterId = getClusterId();

    Response response =
        request("/v3/clusters/" + clusterId + "/topics")
            .accept(MediaType.APPLICATION_JSON)
            .post(
                Entity.entity(
                    "{\"topic_name\":\""
                        + TOPIC_1
                        + "\",\"partitions_count\":2,\\"
                        + "replication_factor\":1}",
                    MediaType.APPLICATION_JSON));
    assertEquals(Status.BAD_REQUEST.getStatusCode(), response.getStatus());
  }

  @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
  @ValueSource(strings = {"kraft", "zk"})
  public void validateOnlyCreateTopic_existingTopic_returnsBadRequest(String quorum) {
    String clusterId = getClusterId();

    Response response =
        request("/v3/clusters/" + clusterId + "/topics")
            .accept(MediaType.APPLICATION_JSON)
            .post(
                Entity.entity(
                    "{\"topic_name\":\""
                        + TOPIC_1
                        + "\",\"partitions_count\":2,\\"
                        + "replication_factor\":1,"
                        + "\"validate_only\":true}",
                    MediaType.APPLICATION_JSON));
    assertEquals(Status.BAD_REQUEST.getStatusCode(), response.getStatus());
  }

  @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
  @ValueSource(strings = {"kraft", "zk"})
  public void createTopic_nonExistingCluster_returnsNotFound(String quorum) {
    Response response =
        request("/v3/clusters/foobar/topics")
            .accept(MediaType.APPLICATION_JSON)
            .post(
                Entity.entity(
                    "{\"topic_name\":\"topic-4\",\"partitions_count\":1,\"replication_factor\":1}",
                    MediaType.APPLICATION_JSON));
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
  @ValueSource(strings = {"kraft", "zk"})
  public void createTopic_withoutRequestBody_returnsInvalidPayload(String quorum) {
    String clusterId = getClusterId();
    Response response =
        request("/v3/clusters/" + clusterId + "/topics")
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(null, MediaType.APPLICATION_JSON));
    assertEquals(422, response.getStatus());
  }

  @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
  @ValueSource(strings = {"kraft", "zk"})
  public void deleteTopic_existingTopic_deletesTopic(String quorum) {
    String clusterId = getClusterId();

    Response response =
        request("/v3/clusters/" + clusterId + "/topics/" + TOPIC_1)
            .accept(MediaType.APPLICATION_JSON)
            .delete();
    assertEquals(Status.NO_CONTENT.getStatusCode(), response.getStatus());
    assertTrue(response.readEntity(String.class).isEmpty());
    testWithRetry(
        () ->
            assertFalse(
                getTopicNames().contains(TOPIC_1),
                String.format("Topic names should not contain %s after its deletion", TOPIC_1)));
  }

  @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
  @ValueSource(strings = {"kraft", "zk"})
  public void deleteTopic_nonExistingTopic_returnsNotFound(String quorum) {
    String clusterId = getClusterId();

    Response response =
        request("/v3/clusters/" + clusterId + "/topics/foobar")
            .accept(MediaType.APPLICATION_JSON)
            .delete();
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
  @ValueSource(strings = {"kraft", "zk"})
  public void deleteTopic_nonExistingCluster_returnsNotFound(String quorum) {
    Response response =
        request("/v3/clusters/foobar/topics/" + TOPIC_1)
            .accept(MediaType.APPLICATION_JSON)
            .delete();
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
  @ValueSource(strings = {"kraft", "zk"})
  public void deleteTopic_nonExistingCluster_noContentType_returnsNotFound(String quorum) {
    Response response = request("/v3/clusters/foobar/topics/" + TOPIC_1).delete();
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
  @ValueSource(strings = {"kraft", "zk"})
  public void createAndDelete_nonExisting_returnsNotFoundCreatedAndNotFound(String quorum) {
    String baseUrl = restConnect;
    String clusterId = getClusterId();
    String topicName = "topic-4";

    Response nonExistingGetTopicResponse =
        request("/v3/clusters/" + clusterId + "/topics/" + topicName)
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(Status.NOT_FOUND.getStatusCode(), nonExistingGetTopicResponse.getStatus());

    CreateTopicResponse expectedCreateTopicResponse =
        CreateTopicResponse.create(
            TopicData.builder()
                .setMetadata(
                    Resource.Metadata.builder()
                        .setSelf(baseUrl + "/v3/clusters/" + clusterId + "/topics/" + topicName)
                        .setResourceName("crn:///kafka=" + clusterId + "/topic=" + topicName)
                        .build())
                .setClusterId(clusterId)
                .setTopicName(topicName)
                .setInternal(false)
                .setReplicationFactor(2)
                .setPartitionsCount(1)
                .setPartitions(
                    Resource.Relationship.create(
                        baseUrl
                            + "/v3/clusters/"
                            + clusterId
                            + "/topics/"
                            + topicName
                            + "/partitions"))
                .setConfigs(
                    Resource.Relationship.create(
                        baseUrl
                            + "/v3/clusters/"
                            + clusterId
                            + "/topics/"
                            + topicName
                            + "/configs"))
                .setPartitionReassignments(
                    Resource.Relationship.create(
                        baseUrl
                            + "/v3/clusters/"
                            + clusterId
                            + "/topics/"
                            + topicName
                            + "/partitions/-/reassignment"))
                .setAuthorizedOperations(emptySet())
                .build());

    Response createTopicResponse =
        request("/v3/clusters/" + clusterId + "/topics")
            .accept(MediaType.APPLICATION_JSON)
            .post(
                Entity.entity(
                    "{\"topic_name\":\""
                        + topicName
                        + "\",\"partitions_count\":1,"
                        + "\"configs\":[{\"name\":\"cleanup.policy\",\"value\":\"compact\"}]}",
                    MediaType.APPLICATION_JSON));
    assertEquals(Status.CREATED.getStatusCode(), createTopicResponse.getStatus());

    CreateTopicResponse actualCreateTopicResponse =
        createTopicResponse.readEntity(CreateTopicResponse.class);

    assertEquals(expectedCreateTopicResponse, actualCreateTopicResponse);
    testWithRetry(
        () ->
            assertTrue(
                getTopicNames().contains(topicName),
                String.format("Topic names should contain %s after its creation", topicName)));

    GetTopicResponse expectedExistingGetTopicResponse =
        GetTopicResponse.create(
            TopicData.builder()
                .setMetadata(
                    Resource.Metadata.builder()
                        .setSelf(baseUrl + "/v3/clusters/" + clusterId + "/topics/" + topicName)
                        .setResourceName("crn:///kafka=" + clusterId + "/topic=" + topicName)
                        .build())
                .setClusterId(clusterId)
                .setTopicName(topicName)
                .setInternal(false)
                .setReplicationFactor(2)
                .setPartitionsCount(1)
                .setPartitions(
                    Resource.Relationship.create(
                        baseUrl
                            + "/v3/clusters/"
                            + clusterId
                            + "/topics/"
                            + topicName
                            + "/partitions"))
                .setConfigs(
                    Resource.Relationship.create(
                        baseUrl
                            + "/v3/clusters/"
                            + clusterId
                            + "/topics/"
                            + topicName
                            + "/configs"))
                .setPartitionReassignments(
                    Resource.Relationship.create(
                        baseUrl
                            + "/v3/clusters/"
                            + clusterId
                            + "/topics/"
                            + topicName
                            + "/partitions/-/reassignment"))
                .setAuthorizedOperations(emptySet())
                .build());

    testWithRetry(
        () -> {
          Response existingTopicResponse =
              request("/v3/clusters/" + clusterId + "/topics/" + topicName)
                  .accept(MediaType.APPLICATION_JSON)
                  .get();
          assertEquals(Status.OK.getStatusCode(), existingTopicResponse.getStatus());

          GetTopicResponse actualExistingGetTopicResponse =
              existingTopicResponse.readEntity(GetTopicResponse.class);
          assertEquals(expectedExistingGetTopicResponse, actualExistingGetTopicResponse);
        });

    GetTopicConfigResponse expectedExistingGetTopicConfigResponse =
        GetTopicConfigResponse.create(
            TopicConfigData.builder()
                .setMetadata(
                    Resource.Metadata.builder()
                        .setSelf(
                            baseUrl
                                + "/v3/clusters/"
                                + clusterId
                                + "/topics/"
                                + topicName
                                + "/configs/cleanup.policy")
                        .setResourceName(
                            "crn:///kafka="
                                + clusterId
                                + "/topic="
                                + topicName
                                + "/config=cleanup.policy")
                        .build())
                .setClusterId(clusterId)
                .setTopicName(topicName)
                .setName("cleanup.policy")
                .setValue("compact")
                .setDefault(false)
                .setReadOnly(false)
                .setSensitive(false)
                .setSource(ConfigSource.DYNAMIC_TOPIC_CONFIG)
                .setSynonyms(
                    Arrays.asList(
                        ConfigSynonymData.builder()
                            .setName("cleanup.policy")
                            .setValue("compact")
                            .setSource(ConfigSource.DYNAMIC_TOPIC_CONFIG)
                            .build(),
                        ConfigSynonymData.builder()
                            .setName("log.cleanup.policy")
                            .setValue("delete")
                            .setSource(ConfigSource.DEFAULT_CONFIG)
                            .build()))
                .build());

    testWithRetry(
        () -> {
          Response existingGetTopicConfigResponse =
              request(
                      "/v3/clusters/"
                          + clusterId
                          + "/topics/"
                          + topicName
                          + "/configs/cleanup.policy")
                  .accept(MediaType.APPLICATION_JSON)
                  .get();
          assertEquals(Status.OK.getStatusCode(), existingGetTopicConfigResponse.getStatus());

          GetTopicConfigResponse actualGetTopicConfigResponse =
              existingGetTopicConfigResponse.readEntity(GetTopicConfigResponse.class);
          assertEquals(expectedExistingGetTopicConfigResponse, actualGetTopicConfigResponse);
        });

    Response deleteTopicResponse =
        request("/v3/clusters/" + clusterId + "/topics/" + topicName)
            .accept(MediaType.APPLICATION_JSON)
            .delete();
    assertEquals(Status.NO_CONTENT.getStatusCode(), deleteTopicResponse.getStatus());
    assertTrue(deleteTopicResponse.readEntity(String.class).isEmpty());
    testWithRetry(
        () ->
            assertFalse(
                getTopicNames().contains(topicName),
                String.format("Topic names should not contain %s after its deletion", topicName)));

    Response deletedGetTopicResponse =
        request("/v3/clusters/" + clusterId + "/topics/" + topicName)
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(Status.NOT_FOUND.getStatusCode(), deletedGetTopicResponse.getStatus());
  }

  @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
  @ValueSource(strings = {"kraft", "zk"})
  public void updateTopicPartitions_IncreasePartitionsCount_returnsTopicWithIncreasedPartitions(
      String quorum) {
    String baseUrl = restConnect;
    String clusterId = getClusterId();

    GetTopicResponse expected =
        GetTopicResponse.create(
            TopicData.builder()
                .setMetadata(
                    Resource.Metadata.builder()
                        .setSelf(baseUrl + "/v3/clusters/" + clusterId + "/topics/" + TOPIC_1)
                        .setResourceName("crn:///kafka=" + clusterId + "/topic=" + TOPIC_1)
                        .build())
                .setClusterId(clusterId)
                .setTopicName(TOPIC_1)
                .setInternal(false)
                .setReplicationFactor(1)
                .setPartitionsCount(3)
                .setPartitions(
                    Resource.Relationship.create(
                        baseUrl
                            + "/v3/clusters/"
                            + clusterId
                            + "/topics/"
                            + TOPIC_1
                            + "/partitions"))
                .setConfigs(
                    Resource.Relationship.create(
                        baseUrl + "/v3/clusters/" + clusterId + "/topics/" + TOPIC_1 + "/configs"))
                .setPartitionReassignments(
                    Resource.Relationship.create(
                        baseUrl
                            + "/v3/clusters/"
                            + clusterId
                            + "/topics/"
                            + TOPIC_1
                            + "/partitions/-/reassignment"))
                .setAuthorizedOperations(emptySet())
                .build());

    Response getTopicResponse =
        request("/v3/clusters/" + clusterId + "/topics/" + TOPIC_1, USE_ALTERNATE_CLIENT_PROVIDER)
            .accept(MediaType.APPLICATION_JSON)
            .method(
                HttpMethod.PATCH,
                Entity.entity("{\"partitions_count\":3}", MediaType.APPLICATION_JSON));
    assertEquals(Status.OK.getStatusCode(), getTopicResponse.getStatus());

    GetTopicResponse actualCreateTopicResponse =
        getTopicResponse.readEntity(GetTopicResponse.class);

    assertEquals(expected, actualCreateTopicResponse);
  }

  @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
  @ValueSource(strings = {"kraft", "zk"})
  public void updateTopicPartitions_decreasePartitionsCount_returns40002(String quorum) {
    String clusterId = getClusterId();

    Response getTopicResponse =
        request("/v3/clusters/" + clusterId + "/topics/" + TOPIC_1, USE_ALTERNATE_CLIENT_PROVIDER)
            .accept(MediaType.APPLICATION_JSON)
            .method(
                HttpMethod.PATCH,
                Entity.entity("{\"partitions_count\":1}", MediaType.APPLICATION_JSON));
    assertEquals(Status.BAD_REQUEST.getStatusCode(), getTopicResponse.getStatus());
  }

  @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
  @ValueSource(strings = {"kraft", "zk"})
  public void updateTopicPartitions_samePartitionsCount_returns40002(String quorum) {
    String clusterId = getClusterId();

    Response getTopicResponse =
        request("/v3/clusters/" + clusterId + "/topics/" + TOPIC_1, USE_ALTERNATE_CLIENT_PROVIDER)
            .accept(MediaType.APPLICATION_JSON)
            .method(
                HttpMethod.PATCH,
                Entity.entity("{\"partitions_count\":2}", MediaType.APPLICATION_JSON));
    assertEquals(Status.BAD_REQUEST.getStatusCode(), getTopicResponse.getStatus());
  }

  @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
  @ValueSource(strings = {"kraft", "zk"})
  public void updateTopicPartitions_topicDoesntExist_returns404(String quorum) {
    String clusterId = getClusterId();

    Response getTopicResponse =
        request(
                "/v3/clusters/" + clusterId + "/topics/" + TOPIC_NON_EXISTENT,
                USE_ALTERNATE_CLIENT_PROVIDER)
            .accept(MediaType.APPLICATION_JSON)
            .method(
                HttpMethod.PATCH,
                Entity.entity("{\"partitions_count\":1}", MediaType.APPLICATION_JSON));
    assertEquals(Status.NOT_FOUND.getStatusCode(), getTopicResponse.getStatus());
  }
}
