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

package io.confluent.kafkarest.controllers;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

import io.confluent.kafkarest.TestUtils;
import io.confluent.kafkarest.entities.Cluster;
import io.confluent.kafkarest.entities.TopicConfiguration;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import javax.ws.rs.NotFoundException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.AlterConfigsResult;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.easymock.EasyMockRule;
import org.easymock.Mock;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TopicConfigurationManagerImplTest {

  private static final String CLUSTER_ID = "cluster-1";
  private static final String TOPIC_NAME = "topic-1";

  private static final Cluster CLUSTER =
      new Cluster(CLUSTER_ID, /* controller= */ null, emptyList());

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

  private static final Config CONFIG =
      new Config(
          Arrays.asList(
              new ConfigEntry(
                  CONFIGURATION_1.getName(),
                  CONFIGURATION_1.getValue(),
                  CONFIGURATION_1.isDefault(),
                  CONFIGURATION_1.isSensitive(),
                  CONFIGURATION_1.isReadOnly()),
              new ConfigEntry(
                  CONFIGURATION_2.getName(),
                  CONFIGURATION_2.getValue(),
                  CONFIGURATION_2.isDefault(),
                  CONFIGURATION_2.isSensitive(),
                  CONFIGURATION_2.isReadOnly()),
              new ConfigEntry(
                  CONFIGURATION_3.getName(),
                  CONFIGURATION_3.getValue(),
                  CONFIGURATION_3.isDefault(),
                  CONFIGURATION_3.isSensitive(),
                  CONFIGURATION_3.isReadOnly())));

  @Rule
  public final EasyMockRule mocks = new EasyMockRule(this);

  @Mock
  private Admin adminClient;

  @Mock
  private ClusterManager clusterManager;

  @Mock
  private DescribeConfigsResult describeConfigsResult;

  @Mock
  private AlterConfigsResult alterConfigsResult;

  private TopicConfigurationManagerImpl topicConfigurationManager;

  @Before
  public void setUp() {
    topicConfigurationManager = new TopicConfigurationManagerImpl(adminClient, clusterManager);
  }

  @Test
  public void listTopicConfigurations_existingTopic_returnsConfigurations() throws Exception {
    expect(clusterManager.getCluster(CLUSTER_ID)).andReturn(completedFuture(Optional.of(CLUSTER)));
    expect(
        adminClient.describeConfigs(
            singletonList(new ConfigResource(ConfigResource.Type.TOPIC, TOPIC_NAME))))
        .andReturn(describeConfigsResult);
    expect(describeConfigsResult.values())
        .andReturn(
            singletonMap(
                new ConfigResource(ConfigResource.Type.TOPIC, TOPIC_NAME),
                KafkaFuture.completedFuture(CONFIG)));
    replay(adminClient, clusterManager, describeConfigsResult);

    List<TopicConfiguration> configurations =
        topicConfigurationManager.listTopicConfigurations(CLUSTER_ID, TOPIC_NAME).get();

    assertEquals(
        new HashSet<>(Arrays.asList(CONFIGURATION_1, CONFIGURATION_2, CONFIGURATION_3)),
        new HashSet<>(configurations));
  }

  @Test
  public void listTopicConfigurations_nonExistingTopic_throwsNotFound() throws Exception {
    expect(clusterManager.getCluster(CLUSTER_ID)).andReturn(completedFuture(Optional.of(CLUSTER)));
    expect(
        adminClient.describeConfigs(
            singletonList(new ConfigResource(ConfigResource.Type.TOPIC, TOPIC_NAME))))
        .andReturn(describeConfigsResult);
    expect(describeConfigsResult.values())
        .andReturn(
            singletonMap(
                new ConfigResource(ConfigResource.Type.TOPIC, TOPIC_NAME),
                TestUtils.failedFuture(new UnknownTopicOrPartitionException())));
    replay(clusterManager, adminClient, describeConfigsResult);

    try {
      topicConfigurationManager.listTopicConfigurations(CLUSTER_ID, TOPIC_NAME).get();
      fail();
    } catch (ExecutionException e) {
      assertEquals(UnknownTopicOrPartitionException.class, e.getCause().getClass());
    }
  }

  @Test
  public void listTopicConfigurations_nonExistingCluster_throwsNotFound() throws Exception {
    expect(clusterManager.getCluster(CLUSTER_ID)).andReturn(completedFuture(Optional.empty()));
    replay(clusterManager);

    try {
      topicConfigurationManager.listTopicConfigurations(CLUSTER_ID, TOPIC_NAME).get();
      fail();
    } catch (ExecutionException e) {
      assertEquals(NotFoundException.class, e.getCause().getClass());
    }
  }

  @Test
  public void getTopicConfiguration_existingConfiguration_returnsConfiguration() throws Exception {
    expect(clusterManager.getCluster(CLUSTER_ID)).andReturn(completedFuture(Optional.of(CLUSTER)));
    expect(
        adminClient.describeConfigs(
            singletonList(new ConfigResource(ConfigResource.Type.TOPIC, TOPIC_NAME))))
        .andReturn(describeConfigsResult);
    expect(describeConfigsResult.values())
        .andReturn(
            singletonMap(
                new ConfigResource(ConfigResource.Type.TOPIC, TOPIC_NAME),
                KafkaFuture.completedFuture(CONFIG)));
    replay(adminClient, clusterManager, describeConfigsResult);

    TopicConfiguration configuration =
        topicConfigurationManager.getTopicConfiguration(
            CLUSTER_ID, TOPIC_NAME, CONFIGURATION_1.getName())
            .get()
            .get();

    assertEquals(CONFIGURATION_1, configuration);
  }

  @Test
  public void getTopicConfiguration_nonExistingConfiguration_returnsEmpty() throws Exception {
    expect(clusterManager.getCluster(CLUSTER_ID)).andReturn(completedFuture(Optional.of(CLUSTER)));
    expect(
        adminClient.describeConfigs(
            singletonList(new ConfigResource(ConfigResource.Type.TOPIC, TOPIC_NAME))))
        .andReturn(describeConfigsResult);
    expect(describeConfigsResult.values())
        .andReturn(
            singletonMap(
                new ConfigResource(ConfigResource.Type.TOPIC, TOPIC_NAME),
                KafkaFuture.completedFuture(CONFIG)));
    replay(adminClient, clusterManager, describeConfigsResult);

    Optional<TopicConfiguration> configuration =
        topicConfigurationManager.getTopicConfiguration(CLUSTER_ID, TOPIC_NAME, "foobar").get();

    assertFalse(configuration.isPresent());
  }

  @Test
  public void getTopicConfiguration_nonExistingTopic_throwsNotFound() throws Exception {
    expect(clusterManager.getCluster(CLUSTER_ID)).andReturn(completedFuture(Optional.of(CLUSTER)));
    expect(
        adminClient.describeConfigs(
            singletonList(new ConfigResource(ConfigResource.Type.TOPIC, TOPIC_NAME))))
        .andReturn(describeConfigsResult);
    expect(describeConfigsResult.values())
        .andReturn(
            singletonMap(
                new ConfigResource(ConfigResource.Type.TOPIC, TOPIC_NAME),
                TestUtils.failedFuture(new UnknownTopicOrPartitionException())));
    replay(clusterManager, adminClient, describeConfigsResult);

    try {
      topicConfigurationManager.getTopicConfiguration(
          CLUSTER_ID, TOPIC_NAME, CONFIGURATION_1.getName()).get();
      fail();
    } catch (ExecutionException e) {
      assertEquals(UnknownTopicOrPartitionException.class, e.getCause().getClass());
    }
  }

  @Test
  public void getTopicConfiguration_nonExistingCluster_throwsNotFound() throws Exception {
    expect(clusterManager.getCluster(CLUSTER_ID)).andReturn(completedFuture(Optional.empty()));
    replay(clusterManager);

    try {
      topicConfigurationManager.getTopicConfiguration(
          CLUSTER_ID, TOPIC_NAME, CONFIGURATION_1.getName()).get();
      fail();
    } catch (ExecutionException e) {
      assertEquals(NotFoundException.class, e.getCause().getClass());
    }
  }

  @Test
  public void updateTopicConfiguration_existingConfiguration_updatesConfiguration()
      throws Exception {
    expect(clusterManager.getCluster(CLUSTER_ID)).andReturn(completedFuture(Optional.of(CLUSTER)));
    expect(
        adminClient.describeConfigs(
            singletonList(new ConfigResource(ConfigResource.Type.TOPIC, TOPIC_NAME))))
        .andReturn(describeConfigsResult);
    expect(describeConfigsResult.values())
        .andReturn(
            singletonMap(
                new ConfigResource(ConfigResource.Type.TOPIC, TOPIC_NAME),
                KafkaFuture.completedFuture(CONFIG)));
    expect(
        adminClient.incrementalAlterConfigs(
            singletonMap(
                new ConfigResource(ConfigResource.Type.TOPIC, TOPIC_NAME),
                singletonList(
                    new AlterConfigOp(
                        new ConfigEntry(CONFIGURATION_1.getName(), "new-value"),
                        AlterConfigOp.OpType.SET)))))
        .andReturn(alterConfigsResult);
    expect(alterConfigsResult.values())
        .andReturn(
            singletonMap(
                new ConfigResource(ConfigResource.Type.TOPIC, TOPIC_NAME),
                KafkaFuture.completedFuture(null)));
    replay(clusterManager, adminClient, describeConfigsResult, alterConfigsResult);

    topicConfigurationManager.updateTopicConfiguration(
        CLUSTER_ID, TOPIC_NAME, CONFIGURATION_1.getName(), "new-value").get();

    verify(adminClient);
  }

  @Test
  public void updateTopicConfiguration_nonExistingConfiguration_throwsNotFound()
      throws Exception {
    expect(clusterManager.getCluster(CLUSTER_ID)).andReturn(completedFuture(Optional.of(CLUSTER)));
    expect(
        adminClient.describeConfigs(
            singletonList(new ConfigResource(ConfigResource.Type.TOPIC, TOPIC_NAME))))
        .andReturn(describeConfigsResult);
    expect(describeConfigsResult.values())
        .andReturn(
            singletonMap(
                new ConfigResource(ConfigResource.Type.TOPIC, TOPIC_NAME),
                KafkaFuture.completedFuture(CONFIG)));
    replay(clusterManager, adminClient, describeConfigsResult);

    try {
      topicConfigurationManager.updateTopicConfiguration(
          CLUSTER_ID, TOPIC_NAME, "foobar", "new-value").get();
      fail();
    } catch (ExecutionException e) {
      assertEquals(NotFoundException.class, e.getCause().getClass());
    }

    verify(adminClient);
  }

  @Test
  public void updateTopicConfiguration_nonExistingTopic_throwsNotFound()
      throws Exception {
    expect(clusterManager.getCluster(CLUSTER_ID)).andReturn(completedFuture(Optional.of(CLUSTER)));
    expect(
        adminClient.describeConfigs(
            singletonList(new ConfigResource(ConfigResource.Type.TOPIC, TOPIC_NAME))))
        .andReturn(describeConfigsResult);
    expect(describeConfigsResult.values())
        .andReturn(
            singletonMap(
                new ConfigResource(ConfigResource.Type.TOPIC, TOPIC_NAME),
                TestUtils.failedFuture(new UnknownTopicOrPartitionException())));
    replay(clusterManager, adminClient, describeConfigsResult);

    try {
      topicConfigurationManager.updateTopicConfiguration(
          CLUSTER_ID, TOPIC_NAME, CONFIGURATION_1.getName(), "new-value").get();
      fail();
    } catch (ExecutionException e) {
      assertEquals(UnknownTopicOrPartitionException.class, e.getCause().getClass());
    }

    verify(adminClient);
  }

  @Test
  public void updateTopicConfiguration_nonExistingCluster_throwsNotFound()
      throws Exception {
    expect(clusterManager.getCluster(CLUSTER_ID)).andReturn(completedFuture(Optional.empty()));
    replay(clusterManager, adminClient);

    try {
      topicConfigurationManager.updateTopicConfiguration(
          CLUSTER_ID, TOPIC_NAME, CONFIGURATION_1.getName(), "new-value").get();
      fail();
    } catch (ExecutionException e) {
      assertEquals(NotFoundException.class, e.getCause().getClass());
    }

    verify(adminClient);
  }

  @Test
  public void resetTopicConfiguration_existingConfiguration_resetsConfiguration()
      throws Exception {
    expect(clusterManager.getCluster(CLUSTER_ID)).andReturn(completedFuture(Optional.of(CLUSTER)));
    expect(
        adminClient.describeConfigs(
            singletonList(new ConfigResource(ConfigResource.Type.TOPIC, TOPIC_NAME))))
        .andReturn(describeConfigsResult);
    expect(describeConfigsResult.values())
        .andReturn(
            singletonMap(
                new ConfigResource(ConfigResource.Type.TOPIC, TOPIC_NAME),
                KafkaFuture.completedFuture(CONFIG)));
    expect(
        adminClient.incrementalAlterConfigs(
            singletonMap(
                new ConfigResource(ConfigResource.Type.TOPIC, TOPIC_NAME),
                singletonList(
                    new AlterConfigOp(
                        new ConfigEntry(CONFIGURATION_1.getName(), /* value= */ null),
                        AlterConfigOp.OpType.DELETE)))))
        .andReturn(alterConfigsResult);
    expect(alterConfigsResult.values())
        .andReturn(
            singletonMap(
                new ConfigResource(ConfigResource.Type.TOPIC, TOPIC_NAME),
                KafkaFuture.completedFuture(null)));
    replay(clusterManager, adminClient, describeConfigsResult, alterConfigsResult);

    topicConfigurationManager.resetTopicConfiguration(
        CLUSTER_ID, TOPIC_NAME, CONFIGURATION_1.getName()).get();

    verify(adminClient);
  }

  @Test
  public void resetTopicConfiguration_nonExistingConfiguration_throwsNotFound()
      throws Exception {
    expect(clusterManager.getCluster(CLUSTER_ID)).andReturn(completedFuture(Optional.of(CLUSTER)));
    expect(
        adminClient.describeConfigs(
            singletonList(new ConfigResource(ConfigResource.Type.TOPIC, TOPIC_NAME))))
        .andReturn(describeConfigsResult);
    expect(describeConfigsResult.values())
        .andReturn(
            singletonMap(
                new ConfigResource(ConfigResource.Type.TOPIC, TOPIC_NAME),
                KafkaFuture.completedFuture(CONFIG)));
    replay(clusterManager, adminClient, describeConfigsResult);

    try {
      topicConfigurationManager.resetTopicConfiguration(CLUSTER_ID, TOPIC_NAME, "foobar").get();
      fail();
    } catch (ExecutionException e) {
      assertEquals(NotFoundException.class, e.getCause().getClass());
    }

    verify(adminClient);
  }

  @Test
  public void resetTopicConfiguration_nonExistingTopic_throwsNotFound()
      throws Exception {
    expect(clusterManager.getCluster(CLUSTER_ID)).andReturn(completedFuture(Optional.of(CLUSTER)));
    expect(
        adminClient.describeConfigs(
            singletonList(new ConfigResource(ConfigResource.Type.TOPIC, TOPIC_NAME))))
        .andReturn(describeConfigsResult);
    expect(describeConfigsResult.values())
        .andReturn(
            singletonMap(
                new ConfigResource(ConfigResource.Type.TOPIC, TOPIC_NAME),
                TestUtils.failedFuture(new UnknownTopicOrPartitionException())));
    replay(clusterManager, adminClient, describeConfigsResult);

    try {
      topicConfigurationManager.resetTopicConfiguration(
          CLUSTER_ID, TOPIC_NAME, CONFIGURATION_1.getName()).get();
      fail();
    } catch (ExecutionException e) {
      assertEquals(UnknownTopicOrPartitionException.class, e.getCause().getClass());
    }

    verify(adminClient);
  }

  @Test
  public void resetTopicConfiguration_nonExistingCluster_throwsNotFound()
      throws Exception {
    expect(clusterManager.getCluster(CLUSTER_ID)).andReturn(completedFuture(Optional.empty()));
    replay(clusterManager, adminClient);

    try {
      topicConfigurationManager.resetTopicConfiguration(
          CLUSTER_ID, TOPIC_NAME, CONFIGURATION_1.getName()).get();
      fail();
    } catch (ExecutionException e) {
      assertEquals(NotFoundException.class, e.getCause().getClass());
    }

    verify(adminClient);
  }
}
