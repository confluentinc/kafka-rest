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
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

import io.confluent.kafkarest.common.KafkaFutures;
import io.confluent.kafkarest.entities.AlterConfigCommand;
import io.confluent.kafkarest.entities.Cluster;
import io.confluent.kafkarest.entities.ConfigSource;
import io.confluent.kafkarest.entities.TopicConfig;
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
import org.apache.kafka.clients.admin.DescribeConfigsOptions;
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
public class TopicConfigManagerImplTest {

  private static final String CLUSTER_ID = "cluster-1";
  private static final String TOPIC_NAME = "topic-1";

  private static final Cluster CLUSTER =
      Cluster.create(CLUSTER_ID, /* controller= */ null, emptyList());

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
          ConfigSource.UNKNOWN,
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
          ConfigSource.UNKNOWN,
          /* synonyms= */ emptyList());

  private static final Config CONFIG =
      new Config(
          Arrays.asList(
              new ConfigEntry(
                  CONFIG_1.getName(),
                  CONFIG_1.getValue(),
                  CONFIG_1.isDefault(),
                  CONFIG_1.isSensitive(),
                  CONFIG_1.isReadOnly()),
              new ConfigEntry(
                  CONFIG_2.getName(),
                  CONFIG_2.getValue(),
                  CONFIG_2.isDefault(),
                  CONFIG_2.isSensitive(),
                  CONFIG_2.isReadOnly()),
              new ConfigEntry(
                  CONFIG_3.getName(),
                  CONFIG_3.getValue(),
                  CONFIG_3.isDefault(),
                  CONFIG_3.isSensitive(),
                  CONFIG_3.isReadOnly())));

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

  private TopicConfigManagerImpl topicConfigManager;

  @Before
  public void setUp() {
    topicConfigManager = new TopicConfigManagerImpl(adminClient, clusterManager);
  }

  @Test
  public void listTopicConfigs_existingTopic_returnsConfigs() throws Exception {
    expect(clusterManager.getCluster(CLUSTER_ID)).andReturn(completedFuture(Optional.of(CLUSTER)));
    expect(
        adminClient.describeConfigs(
            eq(singletonList(new ConfigResource(ConfigResource.Type.TOPIC, TOPIC_NAME))),
            anyObject(DescribeConfigsOptions.class)))
        .andReturn(describeConfigsResult);
    expect(describeConfigsResult.values())
        .andReturn(
            singletonMap(
                new ConfigResource(ConfigResource.Type.TOPIC, TOPIC_NAME),
                KafkaFuture.completedFuture(CONFIG)));
    replay(adminClient, clusterManager, describeConfigsResult);

    List<TopicConfig> configs = topicConfigManager.listTopicConfigs(CLUSTER_ID, TOPIC_NAME).get();

    assertEquals(
        new HashSet<>(Arrays.asList(CONFIG_1, CONFIG_2, CONFIG_3)), new HashSet<>(configs));
  }

  @Test
  public void listTopicConfigs_nonExistingTopic_throwsNotFound() throws Exception {
    expect(clusterManager.getCluster(CLUSTER_ID)).andReturn(completedFuture(Optional.of(CLUSTER)));
    expect(
        adminClient.describeConfigs(
            eq(singletonList(new ConfigResource(ConfigResource.Type.TOPIC, TOPIC_NAME))),
            anyObject(DescribeConfigsOptions.class)))
        .andReturn(describeConfigsResult);
    expect(describeConfigsResult.values())
        .andReturn(
            singletonMap(
                new ConfigResource(ConfigResource.Type.TOPIC, TOPIC_NAME),
                KafkaFutures.failedFuture(new UnknownTopicOrPartitionException())));
    replay(clusterManager, adminClient, describeConfigsResult);

    try {
      topicConfigManager.listTopicConfigs(CLUSTER_ID, TOPIC_NAME).get();
      fail();
    } catch (ExecutionException e) {
      assertEquals(UnknownTopicOrPartitionException.class, e.getCause().getClass());
    }
  }

  @Test
  public void listTopicConfigs_nonExistingCluster_throwsNotFound() throws Exception {
    expect(clusterManager.getCluster(CLUSTER_ID)).andReturn(completedFuture(Optional.empty()));
    replay(clusterManager);

    try {
      topicConfigManager.listTopicConfigs(CLUSTER_ID, TOPIC_NAME).get();
      fail();
    } catch (ExecutionException e) {
      assertEquals(NotFoundException.class, e.getCause().getClass());
    }
  }

  @Test
  public void getTopicConfig_existingConfig_returnsConfig() throws Exception {
    expect(clusterManager.getCluster(CLUSTER_ID)).andReturn(completedFuture(Optional.of(CLUSTER)));
    expect(
        adminClient.describeConfigs(
            eq(singletonList(new ConfigResource(ConfigResource.Type.TOPIC, TOPIC_NAME))),
            anyObject(DescribeConfigsOptions.class)))
        .andReturn(describeConfigsResult);
    expect(describeConfigsResult.values())
        .andReturn(
            singletonMap(
                new ConfigResource(ConfigResource.Type.TOPIC, TOPIC_NAME),
                KafkaFuture.completedFuture(CONFIG)));
    replay(adminClient, clusterManager, describeConfigsResult);

    TopicConfig config =
        topicConfigManager.getTopicConfig(
            CLUSTER_ID, TOPIC_NAME, CONFIG_1.getName())
            .get()
            .get();

    assertEquals(CONFIG_1, config);
  }

  @Test
  public void getTopicConfig_nonExistingConfig_returnsEmpty() throws Exception {
    expect(clusterManager.getCluster(CLUSTER_ID)).andReturn(completedFuture(Optional.of(CLUSTER)));
    expect(
        adminClient.describeConfigs(
            eq(singletonList(new ConfigResource(ConfigResource.Type.TOPIC, TOPIC_NAME))),
            anyObject(DescribeConfigsOptions.class)))
        .andReturn(describeConfigsResult);
    expect(describeConfigsResult.values())
        .andReturn(
            singletonMap(
                new ConfigResource(ConfigResource.Type.TOPIC, TOPIC_NAME),
                KafkaFuture.completedFuture(CONFIG)));
    replay(adminClient, clusterManager, describeConfigsResult);

    Optional<TopicConfig> config =
        topicConfigManager.getTopicConfig(CLUSTER_ID, TOPIC_NAME, "foobar").get();

    assertFalse(config.isPresent());
  }

  @Test
  public void getTopicConfig_nonExistingTopic_throwsNotFound() throws Exception {
    expect(clusterManager.getCluster(CLUSTER_ID)).andReturn(completedFuture(Optional.of(CLUSTER)));
    expect(
        adminClient.describeConfigs(
            eq(singletonList(new ConfigResource(ConfigResource.Type.TOPIC, TOPIC_NAME))),
            anyObject(DescribeConfigsOptions.class)))
        .andReturn(describeConfigsResult);
    expect(describeConfigsResult.values())
        .andReturn(
            singletonMap(
                new ConfigResource(ConfigResource.Type.TOPIC, TOPIC_NAME),
                KafkaFutures.failedFuture(new UnknownTopicOrPartitionException())));
    replay(clusterManager, adminClient, describeConfigsResult);

    try {
      topicConfigManager.getTopicConfig(
          CLUSTER_ID, TOPIC_NAME, CONFIG_1.getName()).get();
      fail();
    } catch (ExecutionException e) {
      assertEquals(UnknownTopicOrPartitionException.class, e.getCause().getClass());
    }
  }

  @Test
  public void getTopicConfig_nonExistingCluster_throwsNotFound() throws Exception {
    expect(clusterManager.getCluster(CLUSTER_ID)).andReturn(completedFuture(Optional.empty()));
    replay(clusterManager);

    try {
      topicConfigManager.getTopicConfig(
          CLUSTER_ID, TOPIC_NAME, CONFIG_1.getName()).get();
      fail();
    } catch (ExecutionException e) {
      assertEquals(NotFoundException.class, e.getCause().getClass());
    }
  }

  @Test
  public void updateTopicConfig_existingConfig_updatesConfig() throws Exception {
    expect(clusterManager.getCluster(CLUSTER_ID)).andReturn(completedFuture(Optional.of(CLUSTER)));
    expect(
        adminClient.describeConfigs(
            eq(singletonList(new ConfigResource(ConfigResource.Type.TOPIC, TOPIC_NAME))),
            anyObject(DescribeConfigsOptions.class)))
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
                        new ConfigEntry(CONFIG_1.getName(), "new-value"),
                        AlterConfigOp.OpType.SET)))))
        .andReturn(alterConfigsResult);
    expect(alterConfigsResult.values())
        .andReturn(
            singletonMap(
                new ConfigResource(ConfigResource.Type.TOPIC, TOPIC_NAME),
                KafkaFuture.completedFuture(null)));
    replay(clusterManager, adminClient, describeConfigsResult, alterConfigsResult);

    topicConfigManager.updateTopicConfig(
        CLUSTER_ID, TOPIC_NAME, CONFIG_1.getName(), "new-value").get();

    verify(adminClient);
  }

  @Test
  public void updateTopicConfig_nonExistingConfig_throwsNotFound() throws Exception {
    expect(clusterManager.getCluster(CLUSTER_ID)).andReturn(completedFuture(Optional.of(CLUSTER)));
    expect(
        adminClient.describeConfigs(
            eq(singletonList(new ConfigResource(ConfigResource.Type.TOPIC, TOPIC_NAME))),
            anyObject(DescribeConfigsOptions.class)))
        .andReturn(describeConfigsResult);
    expect(describeConfigsResult.values())
        .andReturn(
            singletonMap(
                new ConfigResource(ConfigResource.Type.TOPIC, TOPIC_NAME),
                KafkaFuture.completedFuture(CONFIG)));
    replay(clusterManager, adminClient, describeConfigsResult);

    try {
      topicConfigManager.updateTopicConfig(
          CLUSTER_ID, TOPIC_NAME, "foobar", "new-value").get();
      fail();
    } catch (ExecutionException e) {
      assertEquals(NotFoundException.class, e.getCause().getClass());
    }

    verify(adminClient);
  }

  @Test
  public void updateTopicConfig_nonExistingTopic_throwsNotFound() throws Exception {
    expect(clusterManager.getCluster(CLUSTER_ID)).andReturn(completedFuture(Optional.of(CLUSTER)));
    expect(
        adminClient.describeConfigs(
            eq(singletonList(new ConfigResource(ConfigResource.Type.TOPIC, TOPIC_NAME))),
            anyObject(DescribeConfigsOptions.class)))
        .andReturn(describeConfigsResult);
    expect(describeConfigsResult.values())
        .andReturn(
            singletonMap(
                new ConfigResource(ConfigResource.Type.TOPIC, TOPIC_NAME),
                KafkaFutures.failedFuture(new UnknownTopicOrPartitionException())));
    replay(clusterManager, adminClient, describeConfigsResult);

    try {
      topicConfigManager.updateTopicConfig(
          CLUSTER_ID, TOPIC_NAME, CONFIG_1.getName(), "new-value").get();
      fail();
    } catch (ExecutionException e) {
      assertEquals(UnknownTopicOrPartitionException.class, e.getCause().getClass());
    }

    verify(adminClient);
  }

  @Test
  public void updateTopicConfig_nonExistingCluster_throwsNotFound() throws Exception {
    expect(clusterManager.getCluster(CLUSTER_ID)).andReturn(completedFuture(Optional.empty()));
    replay(clusterManager, adminClient);

    try {
      topicConfigManager.updateTopicConfig(
          CLUSTER_ID, TOPIC_NAME, CONFIG_1.getName(), "new-value").get();
      fail();
    } catch (ExecutionException e) {
      assertEquals(NotFoundException.class, e.getCause().getClass());
    }

    verify(adminClient);
  }

  @Test
  public void resetTopicConfig_existingConfig_resetsConfig() throws Exception {
    expect(clusterManager.getCluster(CLUSTER_ID)).andReturn(completedFuture(Optional.of(CLUSTER)));
    expect(
        adminClient.describeConfigs(
            eq(singletonList(new ConfigResource(ConfigResource.Type.TOPIC, TOPIC_NAME))),
            anyObject(DescribeConfigsOptions.class)))
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
                        new ConfigEntry(CONFIG_1.getName(), /* value= */ null),
                        AlterConfigOp.OpType.DELETE)))))
        .andReturn(alterConfigsResult);
    expect(alterConfigsResult.values())
        .andReturn(
            singletonMap(
                new ConfigResource(ConfigResource.Type.TOPIC, TOPIC_NAME),
                KafkaFuture.completedFuture(null)));
    replay(clusterManager, adminClient, describeConfigsResult, alterConfigsResult);

    topicConfigManager.resetTopicConfig(
        CLUSTER_ID, TOPIC_NAME, CONFIG_1.getName()).get();

    verify(adminClient);
  }

  @Test
  public void resetTopicConfig_nonExistingConfig_throwsNotFound() throws Exception {
    expect(clusterManager.getCluster(CLUSTER_ID)).andReturn(completedFuture(Optional.of(CLUSTER)));
    expect(
        adminClient.describeConfigs(
            eq(singletonList(new ConfigResource(ConfigResource.Type.TOPIC, TOPIC_NAME))),
            anyObject(DescribeConfigsOptions.class)))
        .andReturn(describeConfigsResult);
    expect(describeConfigsResult.values())
        .andReturn(
            singletonMap(
                new ConfigResource(ConfigResource.Type.TOPIC, TOPIC_NAME),
                KafkaFuture.completedFuture(CONFIG)));
    replay(clusterManager, adminClient, describeConfigsResult);

    try {
      topicConfigManager.resetTopicConfig(CLUSTER_ID, TOPIC_NAME, "foobar").get();
      fail();
    } catch (ExecutionException e) {
      assertEquals(NotFoundException.class, e.getCause().getClass());
    }

    verify(adminClient);
  }

  @Test
  public void resetTopicConfig_nonExistingTopic_throwsNotFound() throws Exception {
    expect(clusterManager.getCluster(CLUSTER_ID)).andReturn(completedFuture(Optional.of(CLUSTER)));
    expect(
        adminClient.describeConfigs(
            eq(singletonList(new ConfigResource(ConfigResource.Type.TOPIC, TOPIC_NAME))),
            anyObject(DescribeConfigsOptions.class)))
        .andReturn(describeConfigsResult);
    expect(describeConfigsResult.values())
        .andReturn(
            singletonMap(
                new ConfigResource(ConfigResource.Type.TOPIC, TOPIC_NAME),
                KafkaFutures.failedFuture(new UnknownTopicOrPartitionException())));
    replay(clusterManager, adminClient, describeConfigsResult);

    try {
      topicConfigManager.resetTopicConfig(
          CLUSTER_ID, TOPIC_NAME, CONFIG_1.getName()).get();
      fail();
    } catch (ExecutionException e) {
      assertEquals(UnknownTopicOrPartitionException.class, e.getCause().getClass());
    }

    verify(adminClient);
  }

  @Test
  public void resetTopicConfig_nonExistingCluster_throwsNotFound() throws Exception {
    expect(clusterManager.getCluster(CLUSTER_ID)).andReturn(completedFuture(Optional.empty()));
    replay(clusterManager, adminClient);

    try {
      topicConfigManager.resetTopicConfig(
          CLUSTER_ID, TOPIC_NAME, CONFIG_1.getName()).get();
      fail();
    } catch (ExecutionException e) {
      assertEquals(NotFoundException.class, e.getCause().getClass());
    }

    verify(adminClient);
  }

  @Test
  public void alterTopicConfigs_existingConfigs_alterConfigs() throws Exception {
    expect(clusterManager.getCluster(CLUSTER_ID)).andReturn(completedFuture(Optional.of(CLUSTER)));
    expect(
        adminClient.describeConfigs(
            eq(singletonList(new ConfigResource(ConfigResource.Type.TOPIC, TOPIC_NAME))),
            anyObject(DescribeConfigsOptions.class)))
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
                Arrays.asList(
                    new AlterConfigOp(
                        new ConfigEntry(CONFIG_1.getName(), "new-value"),
                        AlterConfigOp.OpType.SET),
                    new AlterConfigOp(
                        new ConfigEntry(CONFIG_2.getName(), /* value= */ null),
                        AlterConfigOp.OpType.DELETE)))))
        .andReturn(alterConfigsResult);
    expect(alterConfigsResult.values())
        .andReturn(
            singletonMap(
                new ConfigResource(ConfigResource.Type.TOPIC, TOPIC_NAME),
                KafkaFuture.completedFuture(null)));
    replay(clusterManager, adminClient, describeConfigsResult, alterConfigsResult);

    topicConfigManager.alterTopicConfigs(
        CLUSTER_ID,
        TOPIC_NAME,
        Arrays.asList(
            AlterConfigCommand.set(CONFIG_1.getName(), "new-value"),
            AlterConfigCommand.delete(CONFIG_2.getName())))
        .get();

    verify(adminClient);
  }

  @Test
  public void alterTopicConfigs_nonExistingCluster_throwsNotFound() throws Exception {
    expect(clusterManager.getCluster(CLUSTER_ID)).andReturn(completedFuture(Optional.empty()));
    replay(clusterManager);

    try {
      topicConfigManager.alterTopicConfigs(
          CLUSTER_ID,
          TOPIC_NAME,
          Arrays.asList(
              AlterConfigCommand.set(CONFIG_1.getName(), "new-value"),
              AlterConfigCommand.delete(CONFIG_2.getName())))
          .get();
      fail();
    } catch (ExecutionException e) {
      assertEquals(NotFoundException.class, e.getCause().getClass());
    }
  }

  @Test
  public void alterTopicConfigs_nonExistingTopic_alterConfigs() throws Exception {
    expect(clusterManager.getCluster(CLUSTER_ID)).andReturn(completedFuture(Optional.of(CLUSTER)));
    expect(
        adminClient.describeConfigs(
            eq(singletonList(new ConfigResource(ConfigResource.Type.TOPIC, TOPIC_NAME))),
            anyObject(DescribeConfigsOptions.class)))
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
                Arrays.asList(
                    new AlterConfigOp(
                        new ConfigEntry(CONFIG_1.getName(), "new-value"),
                        AlterConfigOp.OpType.SET),
                    new AlterConfigOp(
                        new ConfigEntry(CONFIG_2.getName(), /* value= */ null),
                        AlterConfigOp.OpType.DELETE)))))
        .andReturn(alterConfigsResult);
    expect(alterConfigsResult.values())
        .andReturn(
            singletonMap(
                new ConfigResource(ConfigResource.Type.TOPIC, TOPIC_NAME),
                KafkaFutures.failedFuture(new UnknownTopicOrPartitionException())));
    replay(clusterManager, adminClient, describeConfigsResult, alterConfigsResult);

    try {
      topicConfigManager.alterTopicConfigs(
          CLUSTER_ID,
          TOPIC_NAME,
          Arrays.asList(
              AlterConfigCommand.set(CONFIG_1.getName(), "new-value"),
              AlterConfigCommand.delete(CONFIG_2.getName())))
          .get();
      fail();
    } catch (ExecutionException e) {
      assertEquals(UnknownTopicOrPartitionException.class, e.getCause().getClass());
    }
  }

  @Test
  public void alterTopicConfigs_oneNonExistingConfig_throwsNotFound() throws Exception {
    expect(clusterManager.getCluster(CLUSTER_ID)).andReturn(completedFuture(Optional.of(CLUSTER)));
    expect(
        adminClient.describeConfigs(
            eq(singletonList(new ConfigResource(ConfigResource.Type.TOPIC, TOPIC_NAME))),
            anyObject(DescribeConfigsOptions.class)))
        .andReturn(describeConfigsResult);
    expect(describeConfigsResult.values())
        .andReturn(
            singletonMap(
                new ConfigResource(ConfigResource.Type.TOPIC, TOPIC_NAME),
                KafkaFuture.completedFuture(CONFIG)));
    replay(clusterManager, adminClient, describeConfigsResult);

    try {
      topicConfigManager.alterTopicConfigs(
          CLUSTER_ID,
          TOPIC_NAME,
          Arrays.asList(
              AlterConfigCommand.set(CONFIG_1.getName(), "new-value"),
              AlterConfigCommand.delete("foobar")))
          .get();
      fail();
    } catch (ExecutionException e) {
      assertEquals(NotFoundException.class, e.getCause().getClass());
    }
  }
}
