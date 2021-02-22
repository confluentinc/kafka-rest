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

import io.confluent.kafkarest.entities.ConfigSource;
import io.confluent.kafkarest.entities.v3.*;
import io.confluent.kafkarest.integration.ClusterTestHarness;
import org.junit.Before;
import org.junit.Test;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import java.util.Arrays;

import static io.confluent.kafkarest.TestUtils.testWithRetry;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AllTopicConfigsResourceIntegrationTest extends ClusterTestHarness {

  private static final String TOPIC_1 = "topic-1";
  private static final String TOPIC_2 = "topic-2";

  public AllTopicConfigsResourceIntegrationTest() {
    super(/* numBrokers= */ 1, /* withSchemaRegistry= */ false);
  }

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();

    createTopic(TOPIC_1, 1, (short) 1);
    createTopic(TOPIC_2, 1, (short) 1);
    setTopicConfig(TOPIC_2,"delete.retention.ms","100000");

  }

  @Test
  public void listTopicConfigs_existingTopics_returnsConfigs() {
    String baseUrl = restConnect;
    String clusterId = getClusterId();

    ResourceCollection.Metadata expectedMetadata =
        ResourceCollection.Metadata.builder()
            .setSelf(
                baseUrl
                    + "/v3/clusters/" + clusterId
                    + "/topics-configs")
            .build();

    TopicConfigData expectedTopic1Config1 =
        TopicConfigData.builder()
            .setMetadata(
                Resource.Metadata.builder()
                    .setSelf(
                        baseUrl
                            + "/v3/clusters/" + clusterId
                            + "/topics/" + TOPIC_1
                            + "/configs/cleanup.policy")
                    .setResourceName(
                        "crn:///kafka="
                            + clusterId + "/topic="
                            + TOPIC_1 + "/config=cleanup.policy")
                    .build())
            .setClusterId(clusterId)
            .setTopicName(TOPIC_1)
            .setName("cleanup.policy")
            .setValue("delete")
            .setDefault(true)
            .setReadOnly(false)
            .setSensitive(false)
            .setSource(ConfigSource.DEFAULT_CONFIG)
            .setSynonyms(
                singletonList(
                    ConfigSynonymData.builder()
                        .setName("log.cleanup.policy")
                        .setValue("delete")
                        .setSource(ConfigSource.DEFAULT_CONFIG)
                        .build()))
            .build();
    TopicConfigData expectedTopic1Config2 =
        TopicConfigData.builder()
            .setMetadata(
                Resource.Metadata.builder()
                    .setSelf(
                        baseUrl
                            + "/v3/clusters/" + clusterId
                            + "/topics/" + TOPIC_1
                            + "/configs/compression.type")
                    .setResourceName(
                        "crn:///kafka=" + clusterId
                            + "/topic=" + TOPIC_1
                            + "/config=compression.type")
                    .build())
            .setClusterId(clusterId)
            .setTopicName(TOPIC_1)
            .setName("compression.type")
            .setValue("producer")
            .setDefault(true)
            .setReadOnly(false)
            .setSensitive(false)
            .setSource(ConfigSource.DEFAULT_CONFIG)
            .setSynonyms(
                singletonList(
                    ConfigSynonymData.builder()
                        .setName("compression.type")
                        .setValue("producer")
                        .setSource(ConfigSource.DEFAULT_CONFIG)
                        .build()))
            .build();
    TopicConfigData expectedTopic1Config3 =
        TopicConfigData.builder()
            .setMetadata(
                Resource.Metadata.builder()
                    .setSelf(

                        baseUrl
                            + "/v3/clusters/" + clusterId
                            + "/topics/" + TOPIC_1
                            + "/configs/delete.retention.ms")
                    .setResourceName(
                        "crn:///kafka=" + clusterId
                            + "/topic=" + TOPIC_1
                            + "/config=delete.retention.ms")
                    .build())
            .setClusterId(clusterId)
            .setTopicName(TOPIC_1)
            .setName("delete.retention.ms")
            .setValue("86400000")
            .setDefault(true)
            .setReadOnly(false)
            .setSensitive(false)
            .setSource(ConfigSource.DEFAULT_CONFIG)
            .setSynonyms(
                singletonList(
                    ConfigSynonymData.builder()
                        .setName("log.cleaner.delete.retention.ms")
                        .setValue("86400000")
                        .setSource(ConfigSource.DEFAULT_CONFIG)
                        .build()))
            .build();

    TopicConfigData expectedTopic2Config1 =
        TopicConfigData.builder()
            .setMetadata(
                Resource.Metadata.builder()
                    .setSelf(
                        baseUrl
                            + "/v3/clusters/" + clusterId
                            + "/topics/" + TOPIC_2
                            + "/configs/cleanup.policy")
                    .setResourceName(
                        "crn:///kafka="
                            + clusterId + "/topic="
                            + TOPIC_2 + "/config=cleanup.policy")
                    .build())
            .setClusterId(clusterId)
            .setTopicName(TOPIC_2)
            .setName("cleanup.policy")
            .setValue("delete")
            .setDefault(true)
            .setReadOnly(false)
            .setSensitive(false)
            .setSource(ConfigSource.DEFAULT_CONFIG)
            .setSynonyms(
                singletonList(
                    ConfigSynonymData.builder()
                        .setName("log.cleanup.policy")
                        .setValue("delete")
                        .setSource(ConfigSource.DEFAULT_CONFIG)
                        .build()))
            .build();
    TopicConfigData expectedTopic2Config2 =
        TopicConfigData.builder()
            .setMetadata(
                Resource.Metadata.builder()
                    .setSelf(
                        baseUrl
                            + "/v3/clusters/" + clusterId
                            + "/topics/" + TOPIC_2
                            + "/configs/compression.type")
                    .setResourceName(
                        "crn:///kafka=" + clusterId
                            + "/topic=" + TOPIC_2
                            + "/config=compression.type")
                    .build())
            .setClusterId(clusterId)
            .setTopicName(TOPIC_2)
            .setName("compression.type")
            .setValue("producer")
            .setDefault(true)
            .setReadOnly(false)
            .setSensitive(false)
            .setSource(ConfigSource.DEFAULT_CONFIG)
            .setSynonyms(
                singletonList(
                    ConfigSynonymData.builder()
                        .setName("compression.type")
                        .setValue("producer")
                        .setSource(ConfigSource.DEFAULT_CONFIG)
                        .build()))
            .build();
    TopicConfigData expectedTopic2Config3 =
        TopicConfigData.builder()
            .setMetadata(
                Resource.Metadata.builder()
                    .setSelf(

                        baseUrl
                            + "/v3/clusters/" + clusterId
                            + "/topics/" + TOPIC_2
                            + "/configs/delete.retention.ms")
                    .setResourceName(
                        "crn:///kafka=" + clusterId
                            + "/topic=" + TOPIC_2
                            + "/config=delete.retention.ms")
                    .build())
            .setClusterId(clusterId)
            .setTopicName(TOPIC_2)
            .setName("delete.retention.ms")
            .setValue("100000")
            .setDefault(false)
            .setReadOnly(false)
            .setSensitive(false)
            .setSource(ConfigSource.DYNAMIC_TOPIC_CONFIG)
            .setSynonyms(
                Arrays.asList(
                    ConfigSynonymData.builder()
                        .setName("delete.retention.ms")
                        .setValue("100000")
                        .setSource(ConfigSource.DYNAMIC_TOPIC_CONFIG)
                        .build(),
                    ConfigSynonymData.builder()
                        .setName("log.cleaner.delete.retention.ms")
                        .setValue("86400000")
                        .setSource(ConfigSource.DEFAULT_CONFIG)
                        .build())
            )
            .build();

    Response response =
        request("/v3/clusters/" + clusterId + "/topics-configs")
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(Status.OK.getStatusCode(), response.getStatus());
    ListTopicConfigsResponse responseBody = response.readEntity(ListTopicConfigsResponse.class);
    assertEquals(expectedMetadata, responseBody.getValue().getMetadata());
    assertTrue(
        String.format("Not true that `%s' contains `%s'.", responseBody, expectedTopic1Config1),
        responseBody.getValue().getData().contains(expectedTopic1Config1));
    assertTrue(
        String.format("Not true that `%s' contains `%s'.", responseBody, expectedTopic1Config2),
        responseBody.getValue().getData().contains(expectedTopic1Config2));
    assertTrue(
        String.format("Not true that `%s' contains `%s'.", responseBody, expectedTopic1Config3),
        responseBody.getValue().getData().contains(expectedTopic1Config3));
    assertTrue(
        String.format("Not true that `%s' contains `%s'.", responseBody, expectedTopic2Config1),
        responseBody.getValue().getData().contains(expectedTopic2Config1));
    assertTrue(
        String.format("Not true that `%s' contains `%s'.", responseBody, expectedTopic2Config2),
        responseBody.getValue().getData().contains(expectedTopic2Config2));
    assertTrue(
        String.format("Not true that `%s' contains `%s'.", responseBody, expectedTopic2Config3),
        responseBody.getValue().getData().contains(expectedTopic2Config3));
  }

  @Test
  public void listTopicConfigs_nonExistingCluster_throwsNotFound() {
    Response response =
        request("/v3/clusters/foobar/topics-configs")
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

}
