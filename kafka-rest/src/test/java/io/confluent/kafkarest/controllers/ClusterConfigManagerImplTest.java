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
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.fail;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;

import io.confluent.kafkarest.common.KafkaFutures;
import io.confluent.kafkarest.entities.AlterConfigCommand;
import io.confluent.kafkarest.entities.Cluster;
import io.confluent.kafkarest.entities.ClusterConfig;
import io.confluent.kafkarest.entities.ConfigSource;
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
import org.easymock.EasyMockRule;
import org.easymock.Mock;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ClusterConfigManagerImplTest {

  private static final String CLUSTER_ID = "cluster-1";

  private static final Cluster CLUSTER =
      Cluster.create(CLUSTER_ID, null, emptyList());

  private static final ClusterConfig CONFIG_1 =
      ClusterConfig.create(
          CLUSTER_ID,
          "config-1",
          "value-1",
          /* isDefault= */ false,
          /* isReadOnly= */ false,
          /* isSensitive= */ false,
          ConfigSource.DYNAMIC_DEFAULT_BROKER_CONFIG,
          /* synonyms= */ emptyList(),
          ClusterConfig.Type.BROKER);
  private static final ClusterConfig CONFIG_2 =
      ClusterConfig.create(
          CLUSTER_ID,
          "config-2",
          "value-2",
          /* isDefault= */ false,
          /* isReadOnly= */ false,
          /* isSensitive= */ false,
          ConfigSource.DYNAMIC_DEFAULT_BROKER_CONFIG,
          /* synonyms= */ emptyList(),
          ClusterConfig.Type.BROKER);
  private static final ClusterConfig CONFIG_3 =
      ClusterConfig.create(
          CLUSTER_ID,
          "config-3",
          /* value= */ null,
          /* isDefault= */ false,
          /* isReadOnly= */ false,
          /* isSensitive= */ true,
          ConfigSource.DYNAMIC_DEFAULT_BROKER_CONFIG,
          /* synonyms= */ emptyList(),
          ClusterConfig.Type.BROKER);

  private static final Config CONFIG =
      new Config(
          Arrays.asList(
              new OpenConfigEntry(
                  CONFIG_1.getName(),
                  CONFIG_1.getValue(),
                  ConfigEntry.ConfigSource.DYNAMIC_DEFAULT_BROKER_CONFIG,
                  CONFIG_1.isDefault(),
                  CONFIG_1.isSensitive(),
                  CONFIG_1.isReadOnly()),
              new OpenConfigEntry(
                  CONFIG_2.getName(),
                  CONFIG_2.getValue(),
                  ConfigEntry.ConfigSource.DYNAMIC_DEFAULT_BROKER_CONFIG,
                  CONFIG_2.isDefault(),
                  CONFIG_2.isSensitive(),
                  CONFIG_2.isReadOnly()),
              new OpenConfigEntry(
                  CONFIG_3.getName(),
                  CONFIG_3.getValue(),
                  ConfigEntry.ConfigSource.DYNAMIC_DEFAULT_BROKER_CONFIG,
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

  private ClusterConfigManagerImpl clusterConfigManager;

  @Before
  public void setUp() {
    clusterConfigManager = new ClusterConfigManagerImpl(adminClient, clusterManager);
  }

  @Test
  public void listClusterConfigs_existingCluster_returnsConfigs() throws Exception {
    expect(clusterManager.getCluster(CLUSTER_ID)).andReturn(completedFuture(Optional.of(CLUSTER)));
    expect(
        adminClient.describeConfigs(
            eq(singletonList(new ConfigResource(ConfigResource.Type.BROKER, ""))),
            anyObject(DescribeConfigsOptions.class)))
        .andReturn(describeConfigsResult);
    expect(describeConfigsResult.values())
        .andReturn(
            singletonMap(
                new ConfigResource(ConfigResource.Type.BROKER, ""),
                KafkaFuture.completedFuture(CONFIG)));
    replay(adminClient, clusterManager, describeConfigsResult);

    List<ClusterConfig> configs =
        clusterConfigManager.listClusterConfigs(CLUSTER_ID, ClusterConfig.Type.BROKER).get();

    assertEquals(
        new HashSet<>(Arrays.asList(CONFIG_1, CONFIG_2, CONFIG_3)), new HashSet<>(configs));
  }

  @Test
  public void listClusterConfigs_nonExistingCluster_throwsNotFound() throws Exception {
    expect(clusterManager.getCluster(CLUSTER_ID)).andReturn(completedFuture(Optional.of(CLUSTER)));
    expect(
        adminClient.describeConfigs(
            eq(singletonList(new ConfigResource(ConfigResource.Type.BROKER, ""))),
            anyObject(DescribeConfigsOptions.class)))
        .andReturn(describeConfigsResult);
    expect(describeConfigsResult.values())
        .andReturn(
            singletonMap(
                new ConfigResource(ConfigResource.Type.BROKER, ""),
                KafkaFutures.failedFuture(new NotFoundException("Cluster not found."))));
    replay(clusterManager, adminClient, describeConfigsResult);

    try {
      clusterConfigManager.listClusterConfigs(CLUSTER_ID, ClusterConfig.Type.BROKER).get();
      fail();
    } catch (ExecutionException e) {
      assertEquals(NotFoundException.class, e.getCause().getClass());
    }
  }

  @Test
  public void getClusterConfig_existingConfig_returnsConfig() throws Exception {
    expect(clusterManager.getCluster(CLUSTER_ID)).andReturn(completedFuture(Optional.of(CLUSTER)));
    expect(adminClient.describeConfigs(
        eq(singletonList(new ConfigResource(ConfigResource.Type.BROKER, ""))),
        anyObject(DescribeConfigsOptions.class)))
        .andReturn(describeConfigsResult);
    expect(describeConfigsResult.values())
        .andReturn(
            singletonMap(
                new ConfigResource(ConfigResource.Type.BROKER, ""),
                KafkaFuture.completedFuture(CONFIG)));
    replay(adminClient, clusterManager, describeConfigsResult);

    ClusterConfig config =
        clusterConfigManager.getClusterConfig(
            CLUSTER_ID, ClusterConfig.Type.BROKER, CONFIG_1.getName())
            .get()
            .get();

    assertEquals(CONFIG_1, config);
  }

  @Test
  public void getClusterConfig_nonExistingConfig_returnsEmpty() throws Exception {
    expect(clusterManager.getCluster(CLUSTER_ID)).andReturn(completedFuture(Optional.of(CLUSTER)));
    expect(
        adminClient.describeConfigs(
            eq(singletonList(
                new ConfigResource(ConfigResource.Type.BROKER, ""))),
            anyObject(DescribeConfigsOptions.class)))
        .andReturn(describeConfigsResult);
    expect(describeConfigsResult.values())
        .andReturn(
            singletonMap(
                new ConfigResource(ConfigResource.Type.BROKER, ""),
                KafkaFuture.completedFuture(CONFIG)));
    replay(adminClient, clusterManager, describeConfigsResult);

    Optional<ClusterConfig> config =
        clusterConfigManager.getClusterConfig(CLUSTER_ID, ClusterConfig.Type.BROKER, "foobar")
            .get();

    assertFalse(config.isPresent());
  }

  @Test
  public void getClusterConfig_nonExistingCluster_throwsNotFound() throws Exception {
    expect(clusterManager.getCluster(CLUSTER_ID)).andReturn(completedFuture(Optional.of(CLUSTER)));
    expect(
        adminClient.describeConfigs(
            eq(singletonList(
                new ConfigResource(ConfigResource.Type.BROKER, ""))),
            anyObject(DescribeConfigsOptions.class)))
        .andReturn(describeConfigsResult);
    expect(describeConfigsResult.values())
        .andReturn(
            singletonMap(
                new ConfigResource(ConfigResource.Type.BROKER, ""),
                KafkaFutures.failedFuture(new NotFoundException("Cluster not found."))));
    replay(clusterManager, adminClient, describeConfigsResult);

    try {
      clusterConfigManager.getClusterConfig(
          CLUSTER_ID, ClusterConfig.Type.BROKER, CONFIG_1.getName()).get();
      fail();
    } catch (ExecutionException e) {
      assertEquals(NotFoundException.class, e.getCause().getClass());
    }
  }

  @Test
  public void updateClusterConfig_existingConfig_updatesConfigs() throws Exception {
    expect(clusterManager.getCluster(CLUSTER_ID)).andReturn(completedFuture(Optional.of(CLUSTER)));
    expect(
        adminClient.incrementalAlterConfigs(
            singletonMap(
                new ConfigResource(ConfigResource.Type.BROKER, ""),
                singletonList(
                    new AlterConfigOp(
                        new ConfigEntry(CONFIG_1.getName(), "new-value"),
                        AlterConfigOp.OpType.SET)))))
        .andReturn(alterConfigsResult);
    expect(alterConfigsResult.values())
        .andReturn(
            singletonMap(
                new ConfigResource(ConfigResource.Type.BROKER, ""),
                KafkaFuture.completedFuture(null)));
    replay(clusterManager, adminClient, alterConfigsResult);

    clusterConfigManager.upsertClusterConfig(
        CLUSTER_ID, ClusterConfig.Type.BROKER, CONFIG_1.getName(), "new-value").get();

    verify(adminClient);
  }

  @Test
  public void updateClusterConfig_nonExistingCluster_throwsNotFound() throws Exception {
    expect(clusterManager.getCluster(CLUSTER_ID)).andReturn(completedFuture(Optional.empty()));
    replay(clusterManager);

    try {
      clusterConfigManager.upsertClusterConfig(
          CLUSTER_ID, ClusterConfig.Type.BROKER, CONFIG_1.getName(), "new-value").get();
      fail();
    } catch (ExecutionException e) {
      assertEquals(NotFoundException.class, e.getCause().getClass());
    }

    verify(clusterManager);
  }

  @Test
  public void resetClusterConfig_existingConfig_resetsConfig() throws Exception {
    expect(clusterManager.getCluster(CLUSTER_ID)).andReturn(completedFuture(Optional.of(CLUSTER)));
    expect(
        adminClient.incrementalAlterConfigs(
            singletonMap(
                new ConfigResource(ConfigResource.Type.BROKER, ""),
                singletonList(
                    new AlterConfigOp(
                        new ConfigEntry(CONFIG_1.getName(), /* value= */ null),
                        AlterConfigOp.OpType.DELETE)))))
        .andReturn(alterConfigsResult);
    expect(alterConfigsResult.values())
        .andReturn(
            singletonMap(
                new ConfigResource(ConfigResource.Type.BROKER, ""),
                KafkaFuture.completedFuture(null)));
    replay(clusterManager, adminClient, alterConfigsResult);

    clusterConfigManager.deleteClusterConfig(
        CLUSTER_ID, ClusterConfig.Type.BROKER, CONFIG_1.getName()).get();

    verify(adminClient);
  }

  @Test
  public void resetClusterConfig_nonExistingCluster_throwsNotFound() throws Exception {
    expect(clusterManager.getCluster(CLUSTER_ID)).andReturn(completedFuture(Optional.empty()));
    replay(clusterManager);

    try {
      clusterConfigManager.deleteClusterConfig(
          CLUSTER_ID, ClusterConfig.Type.BROKER, CONFIG_1.getName()).get();
      fail();
    } catch (ExecutionException e) {
      assertEquals(NotFoundException.class, e.getCause().getClass());
    }

    verify(clusterManager);
  }

  @Test
  public void alterClusterConfigs_existingConfigs_alterConfigs() throws Exception {
    expect(clusterManager.getCluster(CLUSTER_ID)).andReturn(completedFuture(Optional.of(CLUSTER)));
    expect(
        adminClient.incrementalAlterConfigs(
            singletonMap(
                new ConfigResource(ConfigResource.Type.BROKER, ""),
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
                new ConfigResource(ConfigResource.Type.BROKER, ""),
                KafkaFuture.completedFuture(null)));
    replay(clusterManager, adminClient, alterConfigsResult);

    clusterConfigManager.alterClusterConfigs(
        CLUSTER_ID,
        ClusterConfig.Type.BROKER,
        Arrays.asList(
            AlterConfigCommand.set(CONFIG_1.getName(), "new-value"),
            AlterConfigCommand.delete(CONFIG_2.getName())))
        .get();

    verify(adminClient);
  }

  @Test
  public void alterClusterConfigs_nonExistingCluster_throwsNotFound() throws Exception {
    expect(clusterManager.getCluster(CLUSTER_ID)).andReturn(completedFuture(Optional.empty()));
    replay(clusterManager);

    try {
      clusterConfigManager.alterClusterConfigs(
          CLUSTER_ID,
          ClusterConfig.Type.BROKER,
          Arrays.asList(
              AlterConfigCommand.set(CONFIG_1.getName(), "new-value"),
              AlterConfigCommand.delete(CONFIG_2.getName())))
          .get();
      fail();
    } catch (ExecutionException e) {
      assertEquals(NotFoundException.class, e.getCause().getClass());
    }
  }

  private static final class OpenConfigEntry extends ConfigEntry {

    private final ConfigEntry.ConfigSource source;

    public OpenConfigEntry(
        String name,
        String value,
        ConfigEntry.ConfigSource source,
        boolean isDefault,
        boolean isReadOnly,
        boolean isSensitive) {
      super(name, value, isDefault, isReadOnly, isSensitive);
      this.source = requireNonNull(source);
    }

    @Override
    public ConfigSource source() {
      return source;
    }
  }
}
