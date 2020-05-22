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

import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafkarest.Versions;
import io.confluent.kafkarest.entities.ConfigSource;
import io.confluent.kafkarest.entities.v3.CollectionLink;
import io.confluent.kafkarest.entities.v3.ConfigSynonymData;
import io.confluent.kafkarest.entities.v3.GetTopicConfigResponse;
import io.confluent.kafkarest.entities.v3.ResourceLink;
import io.confluent.kafkarest.entities.v3.TopicConfigData;
import io.confluent.kafkarest.integration.ClusterTestHarness;
import java.util.Arrays;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import org.junit.Before;
import org.junit.Test;

public class TopicConfigsResourceIntegrationTest extends ClusterTestHarness {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private static final String TOPIC_1 = "topic-1";

  public TopicConfigsResourceIntegrationTest() {
    super(/* numBrokers= */ 1, /* withSchemaRegistry= */ false);
  }

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();

    createTopic(TOPIC_1, 1, (short) 1);
  }

  @Test
  public void listTopicConfigs_existingTopic_returnsConfigs() throws Exception {
    String baseUrl = restConnect;
    String clusterId = getClusterId();

    String expectedLinks =
        OBJECT_MAPPER.writeValueAsString(
            new CollectionLink(
                baseUrl
                    + "/v3/clusters/" + clusterId
                    + "/topics/" + TOPIC_1
                    + "/configs",
                /* next= */ null));
    String expectedConfig1 =
        OBJECT_MAPPER.writeValueAsString(
            new TopicConfigData(
                "crn:///kafka=" + clusterId + "/topic=" + TOPIC_1 + "/config=cleanup.policy",
                new ResourceLink(
                    baseUrl
                        + "/v3/clusters/" + clusterId
                        + "/topics/" + TOPIC_1
                        + "/configs/cleanup.policy"),
                clusterId,
                TOPIC_1,
                "cleanup.policy",
                "delete",
                /* isDefault= */ true,
                /* isReadOnly= */ false,
                /* isSensitive= */ false,
                ConfigSource.DEFAULT_CONFIG,
                singletonList(
                    new ConfigSynonymData(
                        "log.cleanup.policy", "delete", ConfigSource.DEFAULT_CONFIG))));
    String expectedConfig2 =
        OBJECT_MAPPER.writeValueAsString(
            new TopicConfigData(
                "crn:///kafka=" + clusterId
                    + "/topic=" + TOPIC_1
                    + "/config=compression.type",
                new ResourceLink(
                    baseUrl
                        + "/v3/clusters/" + clusterId
                        + "/topics/" + TOPIC_1
                        + "/configs/compression.type"),
                clusterId,
                TOPIC_1,
                "compression.type",
                "producer",
                /* isDefault= */ true,
                /* isReadOnly= */ false,
                /* isSensitive= */ false,
                ConfigSource.DEFAULT_CONFIG,
                singletonList(
                    new ConfigSynonymData(
                        "compression.type", "producer", ConfigSource.DEFAULT_CONFIG))));
    String expectedConfig3 =
        OBJECT_MAPPER.writeValueAsString(
            new TopicConfigData(
                "crn:///kafka=" + clusterId
                    + "/topic=" + TOPIC_1
                    + "/config=delete.retention.ms",
                new ResourceLink(
                    baseUrl
                        + "/v3/clusters/" + clusterId
                        + "/topics/" + TOPIC_1
                        + "/configs/delete.retention.ms"),
                clusterId,
                TOPIC_1,
                "delete.retention.ms",
                "86400000",
                /* isDefault= */ true,
                /* isReadOnly= */ false,
                /* isSensitive= */ false,
                ConfigSource.DEFAULT_CONFIG,
                singletonList(
                    new ConfigSynonymData(
                        "log.cleaner.delete.retention.ms",
                        "86400000",
                        ConfigSource.DEFAULT_CONFIG))));

    Response response =
        request("/v3/clusters/" + clusterId + "/topics/" + TOPIC_1 + "/configs")
            .accept(Versions.JSON_API)
            .get();
    assertEquals(Status.OK.getStatusCode(), response.getStatus());
    String responseBody = response.readEntity(String.class);
    assertTrue(
        String.format("Not true that `%s' contains `%s'.", responseBody, expectedLinks),
        responseBody.contains(expectedLinks));
    assertTrue(
        String.format("Not true that `%s' contains `%s'.", responseBody, expectedConfig1),
        responseBody.contains(expectedConfig1));
    assertTrue(
        String.format("Not true that `%s' contains `%s'.", responseBody, expectedConfig2),
        responseBody.contains(expectedConfig2));
    assertTrue(
        String.format("Not true that `%s' contains `%s'.", responseBody, expectedConfig3),
        responseBody.contains(expectedConfig3));
  }

  @Test
  public void listTopicConfigs_nonExistingTopic_throwsNotFound() {
    String clusterId = getClusterId();

    Response response =
        request("/v3/clusters/" + clusterId + "/topics/foobar/configs")
            .accept(Versions.JSON_API)
            .get();
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @Test
  public void listTopicConfigs_nonExistingCluster_throwsNotFound() {
    Response response =
        request("/v3/clusters/foobar/topics/" + TOPIC_1 + "/configs")
            .accept(Versions.JSON_API)
            .get();
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @Test
  public void getTopicConfig_existingConfig_returnsConfig() {
    String baseUrl = restConnect;
    String clusterId = getClusterId();

    GetTopicConfigResponse expected =
            new GetTopicConfigResponse(
                new TopicConfigData(
                    "crn:///kafka=" + clusterId
                        + "/topic=" + TOPIC_1
                        + "/config=cleanup.policy",
                    new ResourceLink(
                        baseUrl
                            + "/v3/clusters/" + clusterId
                            + "/topics/" + TOPIC_1
                            + "/configs/cleanup.policy"),
                    clusterId,
                    TOPIC_1,
                    "cleanup.policy",
                    "delete",
                    /* isDefault= */ true,
                    /* isReadOnly= */ false,
                    /* isSensitive= */ false,
                    ConfigSource.DEFAULT_CONFIG,
                    singletonList(
                        new ConfigSynonymData(
                            "log.cleanup.policy", "delete", ConfigSource.DEFAULT_CONFIG))));

    Response response =
        request(
            "/v3/clusters/" + clusterId
                + "/topics/" + TOPIC_1
                + "/configs/cleanup.policy")
            .accept(Versions.JSON_API)
            .get();
    assertEquals(Status.OK.getStatusCode(), response.getStatus());

    GetTopicConfigResponse actual =
            response.readEntity(GetTopicConfigResponse.class);
    assertEquals(expected, actual);
  }

  @Test
  public void getTopicConfig_nonExistingConfig_throwsNotFound() {
    String clusterId = getClusterId();

    Response response =
        request("/v3/clusters/" + clusterId + "/topics/" + TOPIC_1 + "/configs/foobar")
            .accept(Versions.JSON_API)
            .get();
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @Test
  public void getTopicConfig_nonExistingTopic_throwsNotFound() {
    String clusterId = getClusterId();

    Response response =
        request("/v3/clusters/" + clusterId + "/topics/foobar/configs/cleanup.policy")
            .accept(Versions.JSON_API)
            .get();
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @Test
  public void getTopicConfig_nonExistingCluster_throwsNotFound() {
    Response response =
        request("/v3/clusters/foobar/topics/" + TOPIC_1 + "/configs/cleanup.policy")
            .accept(Versions.JSON_API)
            .get();
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @Test
  public void getUpdateReset_withExistingConfig() {
    String baseUrl = restConnect;
    String clusterId = getClusterId();

    GetTopicConfigResponse expectedBeforeUpdate =
            new GetTopicConfigResponse(
                new TopicConfigData(
                    "crn:///kafka=" + clusterId
                        + "/topic=" + TOPIC_1
                        + "/config=cleanup.policy",
                    new ResourceLink(
                        baseUrl
                            + "/v3/clusters/" + clusterId
                            + "/topics/" + TOPIC_1
                            + "/configs/cleanup.policy"),
                    clusterId,
                    TOPIC_1,
                    "cleanup.policy",
                    "delete",
                    /* isDefault= */ true,
                    /* isReadOnly= */ false,
                    /* isSensitive= */ false,
                    ConfigSource.DEFAULT_CONFIG,
                    singletonList(
                        new ConfigSynonymData(
                            "log.cleanup.policy", "delete", ConfigSource.DEFAULT_CONFIG))));

    Response responseBeforeUpdate =
        request(
            "/v3/clusters/" + clusterId
                + "/topics/" + TOPIC_1
                + "/configs/cleanup.policy")
            .accept(Versions.JSON_API)
            .get();
    assertEquals(Status.OK.getStatusCode(), responseBeforeUpdate.getStatus());

    GetTopicConfigResponse actualResponseBeforeUpdate =
            responseBeforeUpdate.readEntity(GetTopicConfigResponse.class);

    assertEquals(expectedBeforeUpdate, actualResponseBeforeUpdate);

    Response updateResponse =
        request(
            "/v3/clusters/" + clusterId + "/topics/" + TOPIC_1 + "/configs/cleanup.policy")
            .accept(Versions.JSON_API)
            .put(
                Entity.entity(
                    "{\"data\":{\"attributes\":{\"value\":\"compact\"}}}", Versions.JSON_API));
    assertEquals(Status.NO_CONTENT.getStatusCode(), updateResponse.getStatus());

    GetTopicConfigResponse expectedAfterUpdate =
            new GetTopicConfigResponse(
                new TopicConfigData(
                    "crn:///kafka=" + clusterId
                        + "/topic=" + TOPIC_1
                        + "/config=cleanup.policy",
                    new ResourceLink(
                        baseUrl
                            + "/v3/clusters/" + clusterId
                            + "/topics/" + TOPIC_1
                            + "/configs/cleanup.policy"),
                    clusterId,
                    TOPIC_1,
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

    Response responseAfterUpdate =
        request(
            "/v3/clusters/" + clusterId
                + "/topics/" + TOPIC_1
                + "/configs/cleanup.policy")
            .accept(Versions.JSON_API)
            .get();
    assertEquals(Status.OK.getStatusCode(), responseAfterUpdate.getStatus());

    GetTopicConfigResponse actualResponseAfterUpdate =
            responseAfterUpdate.readEntity(GetTopicConfigResponse.class);
    assertEquals(expectedAfterUpdate, actualResponseAfterUpdate);

    Response resetResponse =
        request(
            "/v3/clusters/" + clusterId + "/topics/" + TOPIC_1 + "/configs/cleanup.policy")
            .accept(Versions.JSON_API)
            .delete();
    assertEquals(Status.NO_CONTENT.getStatusCode(), resetResponse.getStatus());

    GetTopicConfigResponse expectedAfterReset =
            new GetTopicConfigResponse(
                new TopicConfigData(
                    "crn:///kafka=" + clusterId
                        + "/topic=" + TOPIC_1
                        + "/config=cleanup.policy",
                    new ResourceLink(
                        baseUrl
                            + "/v3/clusters/" + clusterId
                            + "/topics/" + TOPIC_1
                            + "/configs/cleanup.policy"),
                    clusterId,
                    TOPIC_1,
                    "cleanup.policy",
                    "delete",
                    /* isDefault= */ true,
                    /* isReadOnly= */ false,
                    /* isSensitive= */ false,
                    ConfigSource.DEFAULT_CONFIG,
                    singletonList(
                        new ConfigSynonymData(
                            "log.cleanup.policy", "delete", ConfigSource.DEFAULT_CONFIG))));

    Response responseAfterReset =
        request(
            "/v3/clusters/" + clusterId
                + "/topics/" + TOPIC_1
                + "/configs/cleanup.policy")
            .accept(Versions.JSON_API)
            .get();
    assertEquals(Status.OK.getStatusCode(), responseAfterReset.getStatus());

    GetTopicConfigResponse actualResponseAfterReset =
            responseAfterReset.readEntity(GetTopicConfigResponse.class);
    assertEquals(expectedAfterReset, actualResponseAfterReset);
  }

  @Test
  public void updateTopicConfig_nonExistingConfig_throwsNotFound() {
    String clusterId = getClusterId();

    Response response =
        request("/v3/clusters/" + clusterId + "/topics/" + TOPIC_1 + "/configs/foobar")
            .accept(Versions.JSON_API)
            .put(
                Entity.entity(
                    "{\"data\":{\"attributes\":{\"value\":\"compact\"}}}", Versions.JSON_API));
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @Test
  public void updateTopicConfig_nonExistingTopic_throwsNotFound() {
    String clusterId = getClusterId();

    Response response =
        request("/v3/clusters/" + clusterId + "/topics/foobar/configs/cleanup.policy")
            .accept(Versions.JSON_API)
            .put(
                Entity.entity(
                    "{\"data\":{\"attributes\":{\"value\":\"compact\"}}}", Versions.JSON_API));
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @Test
  public void updateTopicConfig_nonExistingCluster_throwsNotFound() {
    Response response =
        request("/v3/clusters/foobar/topics/" + TOPIC_1 + "/configs/cleanup.policy")
            .accept(Versions.JSON_API)
            .put(
                Entity.entity(
                    "{\"data\":{\"attributes\":{\"value\":\"compact\"}}}", Versions.JSON_API));
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @Test
  public void updateTopicConfig_nonExistingCluster_noContentType_throwsNotFound() {
    Response response =
        request("/v3/clusters/foobar/topics/" + TOPIC_1 + "/configs/cleanup.policy")
            .put(
                Entity.entity(
                    "{\"data\":{\"attributes\":{\"value\":\"compact\"}}}", Versions.JSON_API));
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @Test
  public void resetTopicConfig_nonExistingConfig_throwsNotFound() {
    String clusterId = getClusterId();

    Response response =
        request("/v3/clusters/" + clusterId + "/topics/" + TOPIC_1 + "/configs/foobar")
            .accept(Versions.JSON_API)
            .delete();
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @Test
  public void resetTopicConfig_nonExistingTopic_throwsNotFound() {
    String clusterId = getClusterId();

    Response response =
        request("/v3/clusters/" + clusterId + "/topics/foobar/configs/cleanup.policy")
            .accept(Versions.JSON_API)
            .delete();
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @Test
  public void resetTopicConfig_nonExistingCluster_throwsNotFound() {
    Response response =
        request("/v3/clusters/foobar/topics/" + TOPIC_1 + "/configs/cleanup.policy")
            .accept(Versions.JSON_API)
            .delete();
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @Test
  public void resetTopicConfig_nonExistingCluster_noContentType_throwsNotFound() {
    Response response =
        request("/v3/clusters/foobar/topics/" + TOPIC_1 + "/configs/cleanup.policy")
            .delete();
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }
}
