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

import static io.confluent.kafkarest.CompletableFutures.failedFuture;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import io.confluent.kafkarest.controllers.TopicConfigurationManager;
import io.confluent.kafkarest.entities.TopicConfiguration;
import io.confluent.kafkarest.entities.v3.CollectionLink;
import io.confluent.kafkarest.entities.v3.GetTopicConfigurationResponse;
import io.confluent.kafkarest.entities.v3.ListTopicConfigurationsResponse;
import io.confluent.kafkarest.entities.v3.ResourceLink;
import io.confluent.kafkarest.entities.v3.TopicConfigurationData;
import io.confluent.kafkarest.entities.v3.UpdateTopicConfigurationRequest;
import io.confluent.kafkarest.response.CrnFactoryImpl;
import io.confluent.kafkarest.response.FakeAsyncResponse;
import io.confluent.kafkarest.response.FakeUrlFactory;
import java.util.Arrays;
import java.util.Optional;
import javax.ws.rs.NotFoundException;
import org.easymock.EasyMockRule;
import org.easymock.Mock;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TopicConfigurationsResourceTest {

  private static final String CLUSTER_ID = "cluster-1";
  private static final String TOPIC_NAME = "topic-1";

  private static final TopicConfiguration CONFIGURATION_1 =
      new TopicConfiguration(
          CLUSTER_ID,
          TOPIC_NAME,
          "config-1",
          "value-1",
          /* isDefault= */ true,
          /* isReadOnly= */ false,
          /* isSensitive= */ false);
  private static final TopicConfiguration CONFIGURATION_2 =
      new TopicConfiguration(
          CLUSTER_ID,
          TOPIC_NAME,
          "config-2",
          "value-2",
          /* isDefault= */ false,
          /* isReadOnly= */ true,
          /* isSensitive= */ false);
  private static final TopicConfiguration CONFIGURATION_3 =
      new TopicConfiguration(
          CLUSTER_ID,
          TOPIC_NAME,
          "config-3",
          null,
          /* isDefault= */ false,
          /* isReadOnly= */ false,
          /* isSensitive= */ true);

  @Rule
  public final EasyMockRule mocks = new EasyMockRule(this);

  @Mock
  private TopicConfigurationManager topicConfigurationManager;

  private TopicConfigurationsResource topicConfigurationsResource;

  @Before
  public void setUp() {
    topicConfigurationsResource =
        new TopicConfigurationsResource(
            topicConfigurationManager,
            new CrnFactoryImpl(/* crnAuthorityConfig= */ ""),
            new FakeUrlFactory());
  }

  @Test
  public void listTopicConfigurations_existingTopic_returnsConfigurations() {
    expect(topicConfigurationManager.listTopicConfigurations(CLUSTER_ID, TOPIC_NAME))
        .andReturn(
            completedFuture(Arrays.asList(CONFIGURATION_1, CONFIGURATION_2, CONFIGURATION_3)));
    replay(topicConfigurationManager);

    FakeAsyncResponse response = new FakeAsyncResponse();
    topicConfigurationsResource.listTopicConfigurations(response, CLUSTER_ID, TOPIC_NAME);

    ListTopicConfigurationsResponse expected =
        new ListTopicConfigurationsResponse(
            new CollectionLink(
                "/v3/clusters/cluster-1/topics/topic-1/configurations", /* next= */ null),
            Arrays.asList(
                new TopicConfigurationData(
                    "crn:///kafka=cluster-1/topic=topic-1/configuration=config-1",
                    new ResourceLink(
                        "/v3/clusters/cluster-1/topics/topic-1/configurations/config-1"),
                    CLUSTER_ID,
                    TOPIC_NAME,
                    CONFIGURATION_1.getName(),
                    CONFIGURATION_1.getValue(),
                    CONFIGURATION_1.isDefault(),
                    CONFIGURATION_1.isReadOnly(),
                    CONFIGURATION_1.isSensitive()),
                new TopicConfigurationData(
                    "crn:///kafka=cluster-1/topic=topic-1/configuration=config-2",
                    new ResourceLink(
                        "/v3/clusters/cluster-1/topics/topic-1/configurations/config-2"),
                    CLUSTER_ID,
                    TOPIC_NAME,
                    CONFIGURATION_2.getName(),
                    CONFIGURATION_2.getValue(),
                    CONFIGURATION_2.isDefault(),
                    CONFIGURATION_2.isReadOnly(),
                    CONFIGURATION_2.isSensitive()),
                new TopicConfigurationData(
                    "crn:///kafka=cluster-1/topic=topic-1/configuration=config-3",
                    new ResourceLink(
                        "/v3/clusters/cluster-1/topics/topic-1/configurations/config-3"),
                    CLUSTER_ID,
                    TOPIC_NAME,
                    CONFIGURATION_3.getName(),
                    CONFIGURATION_3.getValue(),
                    CONFIGURATION_3.isDefault(),
                    CONFIGURATION_3.isReadOnly(),
                    CONFIGURATION_3.isSensitive())));

    assertEquals(expected, response.getValue());
  }

  @Test
  public void listTopicConfigurations_nonExistingTopicOrCluster_throwsNotFound() {
    expect(topicConfigurationManager.listTopicConfigurations(CLUSTER_ID, TOPIC_NAME))
        .andReturn(failedFuture(new NotFoundException()));
    replay(topicConfigurationManager);

    FakeAsyncResponse response = new FakeAsyncResponse();
    topicConfigurationsResource.listTopicConfigurations(response, CLUSTER_ID, TOPIC_NAME);

    assertEquals(NotFoundException.class, response.getException().getClass());
  }

  @Test
  public void getTopicConfiguration_existingConfiguration_returnsConfiguration() {
    expect(
        topicConfigurationManager.getTopicConfiguration(
            CLUSTER_ID, TOPIC_NAME, CONFIGURATION_1.getName()))
        .andReturn(completedFuture(Optional.of(CONFIGURATION_1)));
    replay(topicConfigurationManager);

    FakeAsyncResponse response = new FakeAsyncResponse();
    topicConfigurationsResource.getTopicConfiguration(
        response, CLUSTER_ID, TOPIC_NAME, CONFIGURATION_1.getName());

    GetTopicConfigurationResponse expected =
        new GetTopicConfigurationResponse(
            new TopicConfigurationData(
                "crn:///kafka=cluster-1/topic=topic-1/configuration=config-1",
                new ResourceLink("/v3/clusters/cluster-1/topics/topic-1/configurations/config-1"),
                CLUSTER_ID,
                TOPIC_NAME,
                CONFIGURATION_1.getName(),
                CONFIGURATION_1.getValue(),
                CONFIGURATION_1.isDefault(),
                CONFIGURATION_1.isReadOnly(),
                CONFIGURATION_1.isSensitive()));

    assertEquals(expected, response.getValue());
  }

  @Test
  public void getTopicConfiguration_nonExistingConfiguration_throwsNotFound() {
    expect(
        topicConfigurationManager.getTopicConfiguration(
            CLUSTER_ID, TOPIC_NAME, CONFIGURATION_1.getName()))
        .andReturn(completedFuture(Optional.empty()));
    replay(topicConfigurationManager);

    FakeAsyncResponse response = new FakeAsyncResponse();
    topicConfigurationsResource.getTopicConfiguration(
        response, CLUSTER_ID, TOPIC_NAME, CONFIGURATION_1.getName());

    assertEquals(NotFoundException.class, response.getException().getClass());
  }

  @Test
  public void getTopicConfiguration_nonExistingTopicOrCluster_throwsNotFound() {
    expect(
        topicConfigurationManager.getTopicConfiguration(
            CLUSTER_ID, TOPIC_NAME, CONFIGURATION_1.getName()))
        .andReturn(failedFuture(new NotFoundException()));
    replay(topicConfigurationManager);

    FakeAsyncResponse response = new FakeAsyncResponse();
    topicConfigurationsResource.getTopicConfiguration(
        response, CLUSTER_ID, TOPIC_NAME, CONFIGURATION_1.getName());

    assertEquals(NotFoundException.class, response.getException().getClass());
  }

  @Test
  public void updateTopicConfiguration_existingConfiguration_updatesConfiguration() {
    expect(
        topicConfigurationManager.updateTopicConfiguration(
            CLUSTER_ID,
            TOPIC_NAME,
            CONFIGURATION_1.getName(),
            "new-value"))
        .andReturn(completedFuture(null));
    replay(topicConfigurationManager);

    FakeAsyncResponse response = new FakeAsyncResponse();
    topicConfigurationsResource.updateTopicConfiguration(
        response,
        CLUSTER_ID,
        TOPIC_NAME,
        CONFIGURATION_1.getName(),
        new UpdateTopicConfigurationRequest(
            new UpdateTopicConfigurationRequest.Data(
                new UpdateTopicConfigurationRequest.Data.Attributes("new-value"))));

    assertNull(response.getValue());
    assertNull(response.getException());
    assertTrue(response.isDone());
  }

  @Test
  public void updateTopicConfiguration_nonExistingConfigurationOrTopicOrCluster_throwsNotFound() {
    expect(
        topicConfigurationManager.updateTopicConfiguration(
            CLUSTER_ID,
            TOPIC_NAME,
            CONFIGURATION_1.getName(),
            "new-value"))
        .andReturn(failedFuture(new NotFoundException()));
    replay(topicConfigurationManager);

    FakeAsyncResponse response = new FakeAsyncResponse();
    topicConfigurationsResource.updateTopicConfiguration(
        response,
        CLUSTER_ID,
        TOPIC_NAME,
        CONFIGURATION_1.getName(),
        new UpdateTopicConfigurationRequest(
            new UpdateTopicConfigurationRequest.Data(
                new UpdateTopicConfigurationRequest.Data.Attributes("new-value"))));

    assertEquals(NotFoundException.class, response.getException().getClass());
  }

  @Test
  public void resetTopicConfiguration_existingConfiguration_resetsConfiguration() {
    expect(
        topicConfigurationManager.resetTopicConfiguration(
            CLUSTER_ID,
            TOPIC_NAME,
            CONFIGURATION_1.getName()))
        .andReturn(completedFuture(null));
    replay(topicConfigurationManager);

    FakeAsyncResponse response = new FakeAsyncResponse();
    topicConfigurationsResource.resetTopicConfiguration(
        response, CLUSTER_ID, TOPIC_NAME, CONFIGURATION_1.getName());

    assertNull(response.getValue());
    assertNull(response.getException());
    assertTrue(response.isDone());
  }

  @Test
  public void resetTopicConfiguration_nonExistingConfigurationOrTopicOrCluster_throwsNotFound() {
    expect(
        topicConfigurationManager.resetTopicConfiguration(
            CLUSTER_ID,
            TOPIC_NAME,
            CONFIGURATION_1.getName()))
        .andReturn(failedFuture(new NotFoundException()));
    replay(topicConfigurationManager);

    FakeAsyncResponse response = new FakeAsyncResponse();
    topicConfigurationsResource.resetTopicConfiguration(
        response, CLUSTER_ID, TOPIC_NAME, CONFIGURATION_1.getName());

    assertEquals(NotFoundException.class, response.getException().getClass());
  }
}
