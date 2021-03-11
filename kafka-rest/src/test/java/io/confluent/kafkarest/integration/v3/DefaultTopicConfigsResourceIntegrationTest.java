/*
 * Copyright 2021 Confluent Inc.
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

import io.confluent.kafkarest.entities.ConfigSource;
import io.confluent.kafkarest.entities.v3.*;
import io.confluent.kafkarest.integration.ClusterTestHarness;
import org.junit.Before;
import org.junit.Test;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class DefaultTopicConfigsResourceIntegrationTest extends ClusterTestHarness {

  private static final String EXISTING_TOPIC = "topic-1";
  private static final String NON_EXISTING_TOPIC = "foobar";


  public DefaultTopicConfigsResourceIntegrationTest() {
    super(/* numBrokers= */ 1, /* withSchemaRegistry= */ false);
  }

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();

    createTopic(EXISTING_TOPIC, 1, (short) 1);
  }

  @Test
  public void listTopicConfigs_nonExistingTopic_returnsConfigs() {
    String baseUrl = restConnect;
    String clusterId = getClusterId();

    ResourceCollection.Metadata expectedMetadata =
        ResourceCollection.Metadata.builder()
            .setSelf(
                baseUrl
                    + "/v3/clusters/" + clusterId
                    + "/topics/" + NON_EXISTING_TOPIC
                    + "/default-configs")
            .build();

    TopicConfigData expectedConfig1 =
        TopicConfigData.builder()
            .setMetadata(
                Resource.Metadata.builder()
                    .setSelf(
                        baseUrl
                            + "/v3/clusters/" + clusterId
                            + "/topics/" + NON_EXISTING_TOPIC
                            + "/configs/cleanup.policy")
                    .setResourceName(
                        "crn:///kafka="
                            + clusterId + "/topic="
                            + NON_EXISTING_TOPIC + "/config=cleanup.policy")
                    .build())
            .setClusterId(clusterId)
            .setTopicName(NON_EXISTING_TOPIC)
            .setName("cleanup.policy")
            .setValue("delete")
            .setDefault(true)
            .setReadOnly(false)
            .setSensitive(false)
            .setSource(ConfigSource.DEFAULT_CONFIG)
            .setSynonyms(Collections.emptyList())
            .build();
    TopicConfigData expectedConfig2 =
        TopicConfigData.builder()
            .setMetadata(
                Resource.Metadata.builder()
                    .setSelf(
                        baseUrl
                            + "/v3/clusters/" + clusterId
                            + "/topics/" + NON_EXISTING_TOPIC
                            + "/configs/compression.type")
                    .setResourceName(
                        "crn:///kafka=" + clusterId
                            + "/topic=" + NON_EXISTING_TOPIC
                            + "/config=compression.type")
                    .build())
            .setClusterId(clusterId)
            .setTopicName(NON_EXISTING_TOPIC)
            .setName("compression.type")
            .setValue("producer")
            .setDefault(true)
            .setReadOnly(false)
            .setSensitive(false)
            .setSource(ConfigSource.DEFAULT_CONFIG)
            .setSynonyms(Collections.emptyList())
            .build();
    TopicConfigData expectedConfig3 =
        TopicConfigData.builder()
            .setMetadata(
                Resource.Metadata.builder()
                    .setSelf(

                        baseUrl
                            + "/v3/clusters/" + clusterId
                            + "/topics/" + NON_EXISTING_TOPIC
                            + "/configs/delete.retention.ms")
                    .setResourceName(
                        "crn:///kafka=" + clusterId
                            + "/topic=" + NON_EXISTING_TOPIC
                            + "/config=delete.retention.ms")
                    .build())
            .setClusterId(clusterId)
            .setTopicName(NON_EXISTING_TOPIC)
            .setName("delete.retention.ms")
            .setValue("86400000")
            .setDefault(true)
            .setReadOnly(false)
            .setSensitive(false)
            .setSource(ConfigSource.DEFAULT_CONFIG)
            .setSynonyms(Collections.emptyList())
            .build();

    Response response =
        request("/v3/clusters/" + clusterId + "/topics/"+ NON_EXISTING_TOPIC + "/default-configs")
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(Status.OK.getStatusCode(), response.getStatus());
    ListTopicConfigsResponse responseBody = response.readEntity(ListTopicConfigsResponse.class);
    assertEquals(expectedMetadata, responseBody.getValue().getMetadata());
    assertTrue(
        String.format("Not true that `%s' contains `%s'.", responseBody, expectedConfig1),
        responseBody.getValue().getData().contains(expectedConfig1));
    assertTrue(
        String.format("Not true that `%s' contains `%s'.", responseBody, expectedConfig2),
        responseBody.getValue().getData().contains(expectedConfig2));
    assertTrue(
        String.format("Not true that `%s' contains `%s'.", responseBody, expectedConfig3),
        responseBody.getValue().getData().contains(expectedConfig3));
  }

  @Test
  public void listTopicConfigs_existingTopic_throwsBadRequest() {
    String clusterId = getClusterId();

    Response response =
        request("/v3/clusters/" + clusterId + "/topics/" + EXISTING_TOPIC + "/default-configs")
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(Status.BAD_REQUEST.getStatusCode(), response.getStatus());
  }

  @Test
  public void listTopicConfigs_nonExistingCluster_throwsNotFound() {
    Response response =
        request("/v3/clusters/foobar/topics/" + EXISTING_TOPIC + "/default-configs")
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }
}
