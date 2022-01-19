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

package io.confluent.kafkarest.resources.v3;

import static io.confluent.kafkarest.common.CompletableFutures.failedFuture;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptySet;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.junit.jupiter.api.Assertions.*;

import io.confluent.kafkarest.controllers.TopicConfigManager;
import io.confluent.kafkarest.controllers.TopicManager;
import io.confluent.kafkarest.entities.ConfigSource;
import io.confluent.kafkarest.entities.Topic;
import io.confluent.kafkarest.entities.TopicConfig;
import io.confluent.kafkarest.entities.v3.ConfigSynonymData;
import io.confluent.kafkarest.entities.v3.ListTopicConfigsResponse;
import io.confluent.kafkarest.entities.v3.Resource;
import io.confluent.kafkarest.entities.v3.ResourceCollection;
import io.confluent.kafkarest.entities.v3.TopicConfigData;
import io.confluent.kafkarest.entities.v3.TopicConfigDataList;
import io.confluent.kafkarest.response.CrnFactoryImpl;
import io.confluent.kafkarest.response.FakeAsyncResponse;
import io.confluent.kafkarest.response.FakeUrlFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;
import javax.ws.rs.NotFoundException;
import org.easymock.EasyMockExtension;
import org.easymock.Mock;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(EasyMockExtension.class)
public class ListAllTopicsConfigsActionTest {

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

  @Mock private TopicConfigManager topicConfigManager;

  @Mock private TopicManager topicManager;

  private ListAllTopicsConfigsAction allTopicConfigsResource;

  @BeforeEach
  public void setUp() {
    allTopicConfigsResource =
        new ListAllTopicsConfigsAction(
            () -> topicManager,
            () -> topicConfigManager,
            new CrnFactoryImpl(/* crnAuthorityConfig= */ ""),
            new FakeUrlFactory());
  }

  @Test
  public void listTopicConfigs_existingTopic_returnsConfigs() {
    expect(topicManager.listTopics(CLUSTER_ID))
        .andReturn(
            completedFuture(
                Arrays.asList(
                    Topic.create(
                        CLUSTER_ID, TOPIC_NAME, new ArrayList<>(), (short) 1, false, emptySet()))));

    expect(topicConfigManager.listTopicConfigs(CLUSTER_ID, Arrays.asList(TOPIC_NAME)))
        .andReturn(
            completedFuture(
                new HashMap<String, List<TopicConfig>>() {
                  {
                    put(TOPIC_NAME, Arrays.asList(CONFIG_1, CONFIG_2, CONFIG_3));
                  }
                }));
    replay(topicManager, topicConfigManager);

    FakeAsyncResponse response = new FakeAsyncResponse();
    allTopicConfigsResource.listTopicConfigs(response, CLUSTER_ID);

    ListTopicConfigsResponse expected =
        ListTopicConfigsResponse.create(
            TopicConfigDataList.builder()
                .setMetadata(
                    ResourceCollection.Metadata.builder()
                        .setSelf("/v3/clusters/cluster-1/topics/-/configs")
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
  public void listTopicConfigs_noTopics_returnsEmptyConfigs() {
    expect(topicManager.listTopics(CLUSTER_ID)).andReturn(completedFuture(new ArrayList<>()));

    expect(topicConfigManager.listTopicConfigs(CLUSTER_ID, new ArrayList<>()))
        .andReturn(completedFuture(new HashMap<String, List<TopicConfig>>()));
    replay(topicManager, topicConfigManager);

    FakeAsyncResponse response = new FakeAsyncResponse();
    allTopicConfigsResource.listTopicConfigs(response, CLUSTER_ID);

    ListTopicConfigsResponse expected =
        ListTopicConfigsResponse.create(
            TopicConfigDataList.builder()
                .setMetadata(
                    ResourceCollection.Metadata.builder()
                        .setSelf("/v3/clusters/cluster-1/topics/-/configs")
                        .build())
                .setData(new ArrayList<>())
                .build());

    assertEquals(expected, response.getValue());
  }

  @Test
  public void listTopicConfigs_nonExistingCluster_throwsNotFound() {
    expect(topicManager.listTopics(CLUSTER_ID)).andReturn(failedFuture(new NotFoundException()));
    replay(topicManager);

    FakeAsyncResponse response = new FakeAsyncResponse();
    allTopicConfigsResource.listTopicConfigs(response, CLUSTER_ID);

    assertEquals(NotFoundException.class, response.getException().getClass());
  }
}
