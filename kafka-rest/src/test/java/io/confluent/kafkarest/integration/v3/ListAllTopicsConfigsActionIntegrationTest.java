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

import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.confluent.kafkarest.entities.ConfigSource;
import io.confluent.kafkarest.entities.v3.ConfigSynonymData;
import io.confluent.kafkarest.entities.v3.ListTopicConfigsResponse;
import io.confluent.kafkarest.entities.v3.Resource;
import io.confluent.kafkarest.entities.v3.ResourceCollection;
import io.confluent.kafkarest.entities.v3.TopicConfigData;
import io.confluent.kafkarest.integration.ClusterTestHarness;
import java.util.Arrays;
import java.util.List;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ListAllTopicsConfigsActionIntegrationTest extends ClusterTestHarness {

  private static final String TOPIC_1 = "topic-1";
  private static final String TOPIC_2 = "topic-2";

  public ListAllTopicsConfigsActionIntegrationTest() {
    super(/* numBrokers= */ 1, /* withSchemaRegistry= */ false);
  }

  @BeforeEach
  @Override
  public void setUp() throws Exception {
    super.setUp();

    createTopic(TOPIC_1, 1, (short) 1);
    createTopic(TOPIC_2, 1, (short) 1);
    setTopicConfig(TOPIC_2, "delete.retention.ms", "100000");
  }

  @Test
  public void listTopicConfigs_existingTopics_returnsConfigs() {
    String baseUrl = restConnect;
    String clusterId = getClusterId();

    ResourceCollection.Metadata expectedMetadata =
        ResourceCollection.Metadata.builder()
            .setSelf(baseUrl + "/v3/clusters/" + clusterId + "/topics/-/configs")
            .build();

    TopicConfigData expectedTopic1Config1 =
        createTopicConfigData(
            baseUrl,
            clusterId,
            TOPIC_1,
            "cleanup.policy",
            "delete",
            true,
            ConfigSource.DEFAULT_CONFIG,
            singletonList(
                ConfigSynonymData.builder()
                    .setName("log.cleanup.policy")
                    .setValue("delete")
                    .setSource(ConfigSource.DEFAULT_CONFIG)
                    .build()));
    TopicConfigData expectedTopic1Config2 =
        createTopicConfigData(
            baseUrl,
            clusterId,
            TOPIC_1,
            "compression.type",
            "producer",
            true,
            ConfigSource.DEFAULT_CONFIG,
            singletonList(
                ConfigSynonymData.builder()
                    .setName("compression.type")
                    .setValue("producer")
                    .setSource(ConfigSource.DEFAULT_CONFIG)
                    .build()));
    TopicConfigData expectedTopic1Config3 =
        createTopicConfigData(
            baseUrl,
            clusterId,
            TOPIC_1,
            "delete.retention.ms",
            "86400000",
            true,
            ConfigSource.DEFAULT_CONFIG,
            singletonList(
                ConfigSynonymData.builder()
                    .setName("log.cleaner.delete.retention.ms")
                    .setValue("86400000")
                    .setSource(ConfigSource.DEFAULT_CONFIG)
                    .build()));

    TopicConfigData expectedTopic2Config1 =
        createTopicConfigData(
            baseUrl,
            clusterId,
            TOPIC_2,
            "cleanup.policy",
            "delete",
            true,
            ConfigSource.DEFAULT_CONFIG,
            singletonList(
                ConfigSynonymData.builder()
                    .setName("log.cleanup.policy")
                    .setValue("delete")
                    .setSource(ConfigSource.DEFAULT_CONFIG)
                    .build()));
    TopicConfigData expectedTopic2Config2 =
        createTopicConfigData(
            baseUrl,
            clusterId,
            TOPIC_2,
            "compression.type",
            "producer",
            true,
            ConfigSource.DEFAULT_CONFIG,
            singletonList(
                ConfigSynonymData.builder()
                    .setName("compression.type")
                    .setValue("producer")
                    .setSource(ConfigSource.DEFAULT_CONFIG)
                    .build()));
    TopicConfigData expectedTopic2Config3 =
        createTopicConfigData(
            baseUrl,
            clusterId,
            TOPIC_2,
            "delete.retention.ms",
            "100000",
            false,
            ConfigSource.DYNAMIC_TOPIC_CONFIG,
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
                    .build()));

    Response response =
        request("/v3/clusters/" + clusterId + "/topics/-/configs")
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(Status.OK.getStatusCode(), response.getStatus());
    ListTopicConfigsResponse responseBody = response.readEntity(ListTopicConfigsResponse.class);
    assertEquals(expectedMetadata, responseBody.getValue().getMetadata());
    assertTrue(
        responseBody.getValue().getData().contains(expectedTopic1Config1),
        String.format("Not true that `%s' contains `%s'.", responseBody, expectedTopic1Config1));
    assertTrue(
        responseBody.getValue().getData().contains(expectedTopic1Config2),
        String.format("Not true that `%s' contains `%s'.", responseBody, expectedTopic1Config2));
    assertTrue(
        responseBody.getValue().getData().contains(expectedTopic1Config3),
        String.format("Not true that `%s' contains `%s'.", responseBody, expectedTopic1Config3));
    assertTrue(
        responseBody.getValue().getData().contains(expectedTopic2Config1),
        String.format("Not true that `%s' contains `%s'.", responseBody, expectedTopic2Config1));
    assertTrue(
        responseBody.getValue().getData().contains(expectedTopic2Config2),
        String.format("Not true that `%s' contains `%s'.", responseBody, expectedTopic2Config2));
    assertTrue(
        responseBody.getValue().getData().contains(expectedTopic2Config3),
        String.format("Not true that `%s' contains `%s'.", responseBody, expectedTopic2Config3));
  }

  @Test
  public void listTopicConfigs_nonExistingCluster_throwsNotFound() {
    Response response =
        request("/v3/clusters/foobar/topics/-/configs").accept(MediaType.APPLICATION_JSON).get();
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  private TopicConfigData createTopicConfigData(
      String baseUrl,
      String clusterId,
      String topic1,
      String configName,
      String configValue,
      boolean isDefault,
      ConfigSource source,
      List<ConfigSynonymData> synonyms) {
    return TopicConfigData.builder()
        .setMetadata(
            Resource.Metadata.builder()
                .setSelf(
                    baseUrl
                        + "/v3/clusters/"
                        + clusterId
                        + "/topics/"
                        + topic1
                        + "/configs/"
                        + configName)
                .setResourceName(
                    "crn:///kafka=" + clusterId + "/topic=" + topic1 + "/config=" + configName)
                .build())
        .setClusterId(clusterId)
        .setTopicName(topic1)
        .setName(configName)
        .setValue(configValue)
        .setDefault(isDefault)
        .setReadOnly(false)
        .setSensitive(false)
        .setSource(source)
        .setSynonyms(synonyms)
        .build();
  }
}
