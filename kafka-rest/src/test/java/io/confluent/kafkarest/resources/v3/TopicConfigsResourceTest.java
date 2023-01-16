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

package io.confluent.kafkarest.resources.v3;

import static io.confluent.kafkarest.common.CompletableFutures.failedFuture;
import static java.util.Collections.emptyList;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import io.confluent.kafkarest.controllers.TopicConfigManager;
import io.confluent.kafkarest.entities.ConfigSource;
import io.confluent.kafkarest.entities.TopicConfig;
import io.confluent.kafkarest.entities.v3.ConfigSynonymData;
import io.confluent.kafkarest.entities.v3.GetTopicConfigResponse;
import io.confluent.kafkarest.entities.v3.ListTopicConfigsResponse;
import io.confluent.kafkarest.entities.v3.Resource;
import io.confluent.kafkarest.entities.v3.ResourceCollection;
import io.confluent.kafkarest.entities.v3.TopicConfigData;
import io.confluent.kafkarest.entities.v3.TopicConfigDataList;
import io.confluent.kafkarest.entities.v3.UpdateTopicConfigRequest;
import io.confluent.kafkarest.response.CrnFactoryImpl;
import io.confluent.kafkarest.response.FakeAsyncResponse;
import io.confluent.kafkarest.response.FakeUrlFactory;
import java.util.Arrays;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.ws.rs.NotFoundException;
import org.easymock.EasyMockRule;
import org.easymock.Mock;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TopicConfigsResourceTest {

  private static final String CLUSTER_ID = "cluster-1";
  private static final String TOPIC_NAME = "topic-1";

  private static final TopicConfig CONFIG_1 =
      TopicConfig.create(
          CLUSTER_ID,
          TOPIC_NAME,
          "config-1",
          "value-1",
          /* isDefault= */ true,
          /* isReadOnly= */ false,
          /* isSensitive= */ false,
          ConfigSource.DEFAULT_CONFIG,
          /* synonyms= */ emptyList());
  private static final TopicConfig CONFIG_2 =
      TopicConfig.create(
          CLUSTER_ID,
          TOPIC_NAME,
          "config-2",
          "value-2",
          /* isDefault= */ false,
          /* isReadOnly= */ true,
          /* isSensitive= */ false,
          ConfigSource.DYNAMIC_TOPIC_CONFIG,
          /* synonyms= */ emptyList());
  private static final TopicConfig CONFIG_3 =
      TopicConfig.create(
          CLUSTER_ID,
          TOPIC_NAME,
          "config-3",
          null,
          /* isDefault= */ false,
          /* isReadOnly= */ false,
          /* isSensitive= */ true,
          ConfigSource.DYNAMIC_TOPIC_CONFIG,
          /* synonyms= */ emptyList());

  @Rule
  public final EasyMockRule mocks = new EasyMockRule(this);

  @Mock
  private TopicConfigManager topicConfigManager;

  private TopicConfigsResource topicConfigsResource;

  @Before
  public void setUp() {
    topicConfigsResource =
        new TopicConfigsResource(
            () -> topicConfigManager,
            new CrnFactoryImpl(/* crnAuthorityConfig= */ ""),
            new FakeUrlFactory());
  }

  @Test
  public void listTopicConfigs_existingTopic_returnsConfigs() {
    expect(topicConfigManager.listTopicConfigs(CLUSTER_ID, TOPIC_NAME))
        .andReturn(
            completedFuture(Arrays.asList(CONFIG_1, CONFIG_2, CONFIG_3)));
    replay(topicConfigManager);

    FakeAsyncResponse response = new FakeAsyncResponse();
    topicConfigsResource.listTopicConfigs(response, CLUSTER_ID, TOPIC_NAME);

    ListTopicConfigsResponse expected =
        ListTopicConfigsResponse.create(
            TopicConfigDataList.builder()
                .setMetadata(
                    ResourceCollection.Metadata.builder()
                        .setSelf(
                            "/v3/clusters/cluster-1/topics/topic-1/configs")
                        .build())
                .setData(
                    Arrays.asList(
                        TopicConfigData.builder()
                            .setMetadata(
                                Resource.Metadata.builder()
                                    .setSelf(
                                        "/v3/clusters/cluster-1/topics/topic-1/configs/config-1")
                                    .setResourceName(
                                        "crn:///kafka=cluster-1/topic=topic-1/config=config-1")
                                    .build())
                            .setClusterId(CLUSTER_ID)
                            .setTopicName(TOPIC_NAME)
                            .setName(CONFIG_1.getName())
                            .setValue(CONFIG_1.getValue())
                            .setDefault(CONFIG_1.isDefault())
                            .setReadOnly(CONFIG_1.isReadOnly())
                            .setSensitive(CONFIG_1.isSensitive())
                            .setSource(CONFIG_1.getSource())
                            .setSynonyms(
                                CONFIG_1.getSynonyms().stream()
                                    .map(ConfigSynonymData::fromConfigSynonym)
                                    .collect(Collectors.toList()))
                            .build(),
                        TopicConfigData.builder()
                            .setMetadata(
                                Resource.Metadata.builder()
                                    .setSelf(
                                        "/v3/clusters/cluster-1/topics/topic-1/configs/config-2")
                                    .setResourceName(
                                        "crn:///kafka=cluster-1/topic=topic-1/config=config-2")
                                    .build())
                            .setClusterId(CLUSTER_ID)
                            .setTopicName(TOPIC_NAME)
                            .setName(CONFIG_2.getName())
                            .setValue(CONFIG_2.getValue())
                            .setDefault(CONFIG_2.isDefault())
                            .setReadOnly(CONFIG_2.isReadOnly())
                            .setSensitive(CONFIG_2.isSensitive())
                            .setSource(CONFIG_2.getSource())
                            .setSynonyms(
                                CONFIG_2.getSynonyms().stream()
                                    .map(ConfigSynonymData::fromConfigSynonym)
                                    .collect(Collectors.toList()))
                            .build(),
                        TopicConfigData.builder()
                            .setMetadata(
                                Resource.Metadata.builder()
                                    .setSelf(
                                        "/v3/clusters/cluster-1/topics/topic-1/configs/config-3")
                                    .setResourceName(
                                        "crn:///kafka=cluster-1/topic=topic-1/config=config-3")
                                    .build())
                            .setClusterId(CLUSTER_ID)
                            .setTopicName(TOPIC_NAME)
                            .setName(CONFIG_3.getName())
                            .setValue(CONFIG_3.getValue())
                            .setDefault(CONFIG_3.isDefault())
                            .setReadOnly(CONFIG_3.isReadOnly())
                            .setSensitive(CONFIG_3.isSensitive())
                            .setSource(CONFIG_3.getSource())
                            .setSynonyms(
                                CONFIG_3.getSynonyms().stream()
                                    .map(ConfigSynonymData::fromConfigSynonym)
                                    .collect(Collectors.toList()))
                            .build()))
                .build());

    assertEquals(expected, response.getValue());
  }

  @Test
  public void listTopicConfigs_nonExistingTopicOrCluster_throwsNotFound() {
    expect(topicConfigManager.listTopicConfigs(CLUSTER_ID, TOPIC_NAME))
        .andReturn(failedFuture(new NotFoundException()));
    replay(topicConfigManager);

    FakeAsyncResponse response = new FakeAsyncResponse();
    topicConfigsResource.listTopicConfigs(response, CLUSTER_ID, TOPIC_NAME);

    assertEquals(NotFoundException.class, response.getException().getClass());
  }

  @Test
  public void getTopicConfig_existingConfig_returnsConfig() {
    expect(
        topicConfigManager.getTopicConfig(
            CLUSTER_ID, TOPIC_NAME, CONFIG_1.getName()))
        .andReturn(completedFuture(Optional.of(CONFIG_1)));
    replay(topicConfigManager);

    FakeAsyncResponse response = new FakeAsyncResponse();
    topicConfigsResource.getTopicConfig(response, CLUSTER_ID, TOPIC_NAME, CONFIG_1.getName());

    GetTopicConfigResponse expected =
        GetTopicConfigResponse.create(
            TopicConfigData.builder()
                .setMetadata(
                    Resource.Metadata.builder()
                        .setSelf(
                            "/v3/clusters/cluster-1/topics/topic-1/configs/config-1")
                        .setResourceName(
                            "crn:///kafka=cluster-1/topic=topic-1/config=config-1")
                        .build())
                .setClusterId(CLUSTER_ID)
                .setTopicName(TOPIC_NAME)
                .setName(CONFIG_1.getName())
                .setValue(CONFIG_1.getValue())
                .setDefault(CONFIG_1.isDefault())
                .setReadOnly(CONFIG_1.isReadOnly())
                .setSensitive(CONFIG_1.isSensitive())
                .setSource(CONFIG_1.getSource())
                .setSynonyms(
                    CONFIG_1.getSynonyms().stream()
                        .map(ConfigSynonymData::fromConfigSynonym)
                        .collect(Collectors.toList()))
                .build());

    assertEquals(expected, response.getValue());
  }

  @Test
  public void getTopicConfig_nonExistingConfig_throwsNotFound() {
    expect(
        topicConfigManager.getTopicConfig(
            CLUSTER_ID, TOPIC_NAME, CONFIG_1.getName()))
        .andReturn(completedFuture(Optional.empty()));
    replay(topicConfigManager);

    FakeAsyncResponse response = new FakeAsyncResponse();
    topicConfigsResource.getTopicConfig(
        response, CLUSTER_ID, TOPIC_NAME, CONFIG_1.getName());

    assertEquals(NotFoundException.class, response.getException().getClass());
  }

  @Test
  public void getTopicConfig_nonExistingTopicOrCluster_throwsNotFound() {
    expect(
        topicConfigManager.getTopicConfig(
            CLUSTER_ID, TOPIC_NAME, CONFIG_1.getName()))
        .andReturn(failedFuture(new NotFoundException()));
    replay(topicConfigManager);

    FakeAsyncResponse response = new FakeAsyncResponse();
    topicConfigsResource.getTopicConfig(
        response, CLUSTER_ID, TOPIC_NAME, CONFIG_1.getName());

    assertEquals(NotFoundException.class, response.getException().getClass());
  }

  @Test
  public void updateTopicConfig_existingConfig_updatesConfig() {
    expect(
        topicConfigManager.updateTopicConfig(
            CLUSTER_ID,
            TOPIC_NAME,
            CONFIG_1.getName(),
            "new-value"))
        .andReturn(completedFuture(null));
    replay(topicConfigManager);

    FakeAsyncResponse response = new FakeAsyncResponse();
    topicConfigsResource.updateTopicConfig(
        response,
        CLUSTER_ID,
        TOPIC_NAME,
        CONFIG_1.getName(),
        UpdateTopicConfigRequest.create("new-value"));

    assertNull(response.getValue());
    assertNull(response.getException());
    assertTrue(response.isDone());
  }

  @Test
  public void updateTopicConfig_nonExistingConfigOrTopicOrCluster_throwsNotFound() {
    expect(
        topicConfigManager.updateTopicConfig(
            CLUSTER_ID,
            TOPIC_NAME,
            CONFIG_1.getName(),
            "new-value"))
        .andReturn(failedFuture(new NotFoundException()));
    replay(topicConfigManager);

    FakeAsyncResponse response = new FakeAsyncResponse();
    topicConfigsResource.updateTopicConfig(
        response,
        CLUSTER_ID,
        TOPIC_NAME,
        CONFIG_1.getName(),
        UpdateTopicConfigRequest.create("new-value"));

    assertEquals(NotFoundException.class, response.getException().getClass());
  }

  @Test
  public void resetTopicConfig_existingConfig_resetsConfig() {
    expect(
        topicConfigManager.resetTopicConfig(
            CLUSTER_ID,
            TOPIC_NAME,
            CONFIG_1.getName()))
        .andReturn(completedFuture(null));
    replay(topicConfigManager);

    FakeAsyncResponse response = new FakeAsyncResponse();
    topicConfigsResource.resetTopicConfig(
        response, CLUSTER_ID, TOPIC_NAME, CONFIG_1.getName());

    assertNull(response.getValue());
    assertNull(response.getException());
    assertTrue(response.isDone());
  }

  @Test
  public void resetTopicConfig_nonExistingConfigOrTopicOrCluster_throwsNotFound() {
    expect(
        topicConfigManager.resetTopicConfig(
            CLUSTER_ID,
            TOPIC_NAME,
            CONFIG_1.getName()))
        .andReturn(failedFuture(new NotFoundException()));
    replay(topicConfigManager);

    FakeAsyncResponse response = new FakeAsyncResponse();
    topicConfigsResource.resetTopicConfig(
        response, CLUSTER_ID, TOPIC_NAME, CONFIG_1.getName());

    assertEquals(NotFoundException.class, response.getException().getClass());
  }
}
