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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.fail;

import io.confluent.kafkarest.OpenConfigEntry;
import io.confluent.kafkarest.common.KafkaFutures;
import io.confluent.kafkarest.entities.AlterConfigCommand;
import io.confluent.kafkarest.entities.BrokerConfig;
import io.confluent.kafkarest.entities.Cluster;
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
import org.easymock.EasyMockExtension;
import org.easymock.Mock;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(EasyMockExtension.class)
public class BrokerConfigManagerImplTest {

  private static final String CLUSTER_ID = "cluster-1";
  private static final int BROKER_ID = 1;

  private static final Cluster CLUSTER = Cluster.create(CLUSTER_ID, null, emptyList());

  private static final BrokerConfig CONFIG_1 =
      BrokerConfig.create(
          CLUSTER_ID,
          BROKER_ID,
          "config-1",
          "value-1",
          /* isDefault= */ true,
          /* isReadOnly= */ false,
          /* isSensitive= */ false,
          ConfigSource.DEFAULT_CONFIG,
          /* synonyms= */ emptyList());
  private static final BrokerConfig CONFIG_2 =
      BrokerConfig.create(
          CLUSTER_ID,
          BROKER_ID,
          "config-2",
          "value-2",
          /* isDefault= */ false,
          /* isReadOnly= */ true,
          /* isSensitive= */ false,
          ConfigSource.UNKNOWN,
          /* synonyms= */ emptyList());
  private static final BrokerConfig CONFIG_3 =
      BrokerConfig.create(
          CLUSTER_ID,
          BROKER_ID,
          "config-3",
          "value-3",
          /* isDefault= */ false,
          /* isReadOnly= */ false,
          /* isSensitive= */ true,
          ConfigSource.UNKNOWN,
          /* synonyms= */ emptyList());

  private static final Config CONFIG =
      new Config(
          Arrays.asList(
              new OpenConfigEntry(
                  CONFIG_1.getName(),
                  CONFIG_1.getValue(),
                  ConfigSource.toAdminConfigSource(CONFIG_1.getSource()),
                  CONFIG_1.isSensitive(),
                  CONFIG_1.isReadOnly()),
              new OpenConfigEntry(
                  CONFIG_2.getName(),
                  CONFIG_2.getValue(),
                  ConfigSource.toAdminConfigSource(CONFIG_2.getSource()),
                  CONFIG_2.isSensitive(),
                  CONFIG_2.isReadOnly()),
              new OpenConfigEntry(
                  CONFIG_3.getName(),
                  CONFIG_3.getValue(),
                  ConfigSource.toAdminConfigSource(CONFIG_3.getSource()),
                  CONFIG_3.isSensitive(),
                  CONFIG_3.isReadOnly())));

  @Mock private Admin adminClient;

  @Mock private ClusterManager clusterManager;

  @Mock private DescribeConfigsResult describeConfigsResult;

  @Mock private AlterConfigsResult alterConfigsResult;

  private BrokerConfigManagerImpl brokerConfigManager;

  @BeforeEach
  public void setUp() {
    brokerConfigManager = new BrokerConfigManagerImpl(adminClient, clusterManager);
  }

  @Test
  public void listBrokerConfigs_existingBroker_returnsConfigs() throws Exception {
    expect(clusterManager.getCluster(CLUSTER_ID)).andReturn(completedFuture(Optional.of(CLUSTER)));
    expect(
            adminClient.describeConfigs(
                eq(
                    singletonList(
                        new ConfigResource(ConfigResource.Type.BROKER, String.valueOf(BROKER_ID)))),
                anyObject(DescribeConfigsOptions.class)))
        .andReturn(describeConfigsResult);
    expect(describeConfigsResult.all())
        .andReturn(
            KafkaFuture.completedFuture(
                singletonMap(
                    new ConfigResource(ConfigResource.Type.BROKER, String.valueOf(BROKER_ID)),
                    CONFIG)));
    replay(adminClient, clusterManager, describeConfigsResult);

    List<BrokerConfig> configs = brokerConfigManager.listBrokerConfigs(CLUSTER_ID, BROKER_ID).get();

    assertEquals(
        new HashSet<>(Arrays.asList(CONFIG_1, CONFIG_2, CONFIG_3)), new HashSet<>(configs));
  }

  @Test
  public void listBrokerConfigs_nonExistingBroker_throwsNotFound() throws Exception {
    expect(clusterManager.getCluster(CLUSTER_ID)).andReturn(completedFuture(Optional.of(CLUSTER)));
    expect(
            adminClient.describeConfigs(
                eq(
                    singletonList(
                        new ConfigResource(ConfigResource.Type.BROKER, String.valueOf(BROKER_ID)))),
                anyObject(DescribeConfigsOptions.class)))
        .andReturn(describeConfigsResult);
    expect(describeConfigsResult.all())
        .andReturn(KafkaFutures.failedFuture(new NotFoundException("Broker not found.")));
    replay(clusterManager, adminClient, describeConfigsResult);

    try {
      brokerConfigManager.listBrokerConfigs(CLUSTER_ID, BROKER_ID).get();
      fail();
    } catch (ExecutionException e) {
      assertEquals(NotFoundException.class, e.getCause().getClass());
    }
  }

  @Test
  public void listBrokerConfigs_nonExistingCluster_throwsNotFound() throws Exception {
    expect(clusterManager.getCluster(CLUSTER_ID)).andReturn(completedFuture(Optional.empty()));
    replay(clusterManager);

    try {
      brokerConfigManager.listBrokerConfigs(CLUSTER_ID, BROKER_ID).get();
      fail();
    } catch (ExecutionException e) {
      assertEquals(NotFoundException.class, e.getCause().getClass());
    }
  }

  @Test
  public void getBrokerConfig_existingConfig_returnsConfig() throws Exception {
    expect(clusterManager.getCluster(CLUSTER_ID)).andReturn(completedFuture(Optional.of(CLUSTER)));
    expect(
            adminClient.describeConfigs(
                eq(
                    singletonList(
                        new ConfigResource(ConfigResource.Type.BROKER, String.valueOf(BROKER_ID)))),
                anyObject(DescribeConfigsOptions.class)))
        .andReturn(describeConfigsResult);
    expect(describeConfigsResult.all())
        .andReturn(
            KafkaFuture.completedFuture(
                singletonMap(
                    new ConfigResource(ConfigResource.Type.BROKER, String.valueOf(BROKER_ID)),
                    CONFIG)));
    replay(adminClient, clusterManager, describeConfigsResult);

    BrokerConfig config =
        brokerConfigManager.getBrokerConfig(CLUSTER_ID, BROKER_ID, CONFIG_1.getName()).get().get();

    assertEquals(CONFIG_1, config);
  }

  @Test
  public void getBrokerConfig_nonExistingConfig_returnsEmpty() throws Exception {
    expect(clusterManager.getCluster(CLUSTER_ID)).andReturn(completedFuture(Optional.of(CLUSTER)));
    expect(
            adminClient.describeConfigs(
                eq(
                    singletonList(
                        new ConfigResource(ConfigResource.Type.BROKER, String.valueOf(BROKER_ID)))),
                anyObject(DescribeConfigsOptions.class)))
        .andReturn(describeConfigsResult);
    expect(describeConfigsResult.all())
        .andReturn(
            KafkaFuture.completedFuture(
                singletonMap(
                    new ConfigResource(ConfigResource.Type.BROKER, String.valueOf(BROKER_ID)),
                    CONFIG)));
    replay(adminClient, clusterManager, describeConfigsResult);

    Optional<BrokerConfig> config =
        brokerConfigManager.getBrokerConfig(CLUSTER_ID, BROKER_ID, "foobar").get();

    assertFalse(config.isPresent());
  }

  @Test
  public void getBrokerConfig_nonExistingBroker_throwsNotFound() throws Exception {
    expect(clusterManager.getCluster(CLUSTER_ID)).andReturn(completedFuture(Optional.of(CLUSTER)));
    expect(
            adminClient.describeConfigs(
                eq(
                    singletonList(
                        new ConfigResource(ConfigResource.Type.BROKER, String.valueOf(BROKER_ID)))),
                anyObject(DescribeConfigsOptions.class)))
        .andReturn(describeConfigsResult);
    expect(describeConfigsResult.all())
        .andReturn(KafkaFutures.failedFuture(new NotFoundException("Broker not found.")));
    replay(clusterManager, adminClient, describeConfigsResult);

    try {
      brokerConfigManager.getBrokerConfig(CLUSTER_ID, BROKER_ID, CONFIG_1.getName()).get();
      fail();
    } catch (ExecutionException e) {
      assertEquals(NotFoundException.class, e.getCause().getClass());
    }
  }

  @Test
  public void getBrokerConfig_nonExistingCluster_throwsNotFound() throws Exception {
    expect(clusterManager.getCluster(CLUSTER_ID)).andReturn(completedFuture(Optional.empty()));
    replay(clusterManager);

    try {
      brokerConfigManager.getBrokerConfig(CLUSTER_ID, BROKER_ID, CONFIG_1.getName()).get();
      fail();
    } catch (ExecutionException e) {
      assertEquals(NotFoundException.class, e.getCause().getClass());
    }
  }

  @Test
  public void updateBrokerConfig_existingConfig_updatesConfigs() throws Exception {
    expect(clusterManager.getCluster(CLUSTER_ID)).andReturn(completedFuture(Optional.of(CLUSTER)));
    expect(
            adminClient.describeConfigs(
                eq(
                    singletonList(
                        new ConfigResource(ConfigResource.Type.BROKER, String.valueOf(BROKER_ID)))),
                anyObject(DescribeConfigsOptions.class)))
        .andReturn(describeConfigsResult);
    expect(describeConfigsResult.all())
        .andReturn(
            KafkaFuture.completedFuture(
                singletonMap(
                    new ConfigResource(ConfigResource.Type.BROKER, String.valueOf(BROKER_ID)),
                    CONFIG)));
    expect(
            adminClient.incrementalAlterConfigs(
                singletonMap(
                    new ConfigResource(ConfigResource.Type.BROKER, String.valueOf(BROKER_ID)),
                    singletonList(
                        new AlterConfigOp(
                            new ConfigEntry(CONFIG_1.getName(), "new-value"),
                            AlterConfigOp.OpType.SET)))))
        .andReturn(alterConfigsResult);
    expect(alterConfigsResult.values())
        .andReturn(
            singletonMap(
                new ConfigResource(ConfigResource.Type.BROKER, String.valueOf(BROKER_ID)),
                KafkaFuture.completedFuture(null)));
    replay(clusterManager, adminClient, describeConfigsResult, alterConfigsResult);

    brokerConfigManager
        .updateBrokerConfig(CLUSTER_ID, BROKER_ID, CONFIG_1.getName(), "new-value")
        .get();

    verify(adminClient);
  }

  @Test
  public void updateBrokerConfig_nonExistingConfig_throwsNotFound() throws Exception {
    expect(clusterManager.getCluster(CLUSTER_ID)).andReturn(completedFuture(Optional.of(CLUSTER)));
    expect(
            adminClient.describeConfigs(
                eq(
                    singletonList(
                        new ConfigResource(ConfigResource.Type.BROKER, String.valueOf(BROKER_ID)))),
                anyObject(DescribeConfigsOptions.class)))
        .andReturn(describeConfigsResult);
    expect(describeConfigsResult.all())
        .andReturn(
            KafkaFuture.completedFuture(
                singletonMap(
                    new ConfigResource(ConfigResource.Type.BROKER, String.valueOf(BROKER_ID)),
                    CONFIG)));
    replay(clusterManager, adminClient, describeConfigsResult);

    try {
      brokerConfigManager.updateBrokerConfig(CLUSTER_ID, BROKER_ID, "foobar", "new-value").get();
      fail();
    } catch (ExecutionException e) {
      assertEquals(NotFoundException.class, e.getCause().getClass());
    }

    verify(adminClient);
  }

  @Test
  public void updateBrokerConfig_nonExistingBroker_throwsNotFound() throws Exception {
    expect(clusterManager.getCluster(CLUSTER_ID)).andReturn(completedFuture(Optional.of(CLUSTER)));
    expect(
            adminClient.describeConfigs(
                eq(
                    singletonList(
                        new ConfigResource(ConfigResource.Type.BROKER, String.valueOf(BROKER_ID)))),
                anyObject(DescribeConfigsOptions.class)))
        .andReturn(describeConfigsResult);
    expect(describeConfigsResult.all())
        .andReturn(KafkaFutures.failedFuture(new NotFoundException()));
    replay(clusterManager, adminClient, describeConfigsResult);

    try {
      brokerConfigManager
          .updateBrokerConfig(CLUSTER_ID, BROKER_ID, CONFIG_1.getName(), "new-value")
          .get();
      fail();
    } catch (ExecutionException e) {
      assertEquals(NotFoundException.class, e.getCause().getClass());
    }

    verify(adminClient);
  }

  @Test
  public void updateBrokerConfig_nonExistingCluster_throwsNotFound() throws Exception {
    expect(clusterManager.getCluster(CLUSTER_ID)).andReturn(completedFuture(Optional.empty()));
    replay(clusterManager, adminClient);

    try {
      brokerConfigManager
          .updateBrokerConfig(CLUSTER_ID, BROKER_ID, CONFIG_1.getName(), "new-value")
          .get();
      fail();
    } catch (ExecutionException e) {
      assertEquals(NotFoundException.class, e.getCause().getClass());
    }

    verify(adminClient);
  }

  @Test
  public void resetBrokerConfig_existingConfig_resetsConfig() throws Exception {
    expect(clusterManager.getCluster(CLUSTER_ID)).andReturn(completedFuture(Optional.of(CLUSTER)));
    expect(
            adminClient.describeConfigs(
                eq(
                    singletonList(
                        new ConfigResource(ConfigResource.Type.BROKER, String.valueOf(BROKER_ID)))),
                anyObject(DescribeConfigsOptions.class)))
        .andReturn(describeConfigsResult);
    expect(describeConfigsResult.all())
        .andReturn(
            KafkaFuture.completedFuture(
                singletonMap(
                    new ConfigResource(ConfigResource.Type.BROKER, String.valueOf(BROKER_ID)),
                    CONFIG)));
    expect(
            adminClient.incrementalAlterConfigs(
                singletonMap(
                    new ConfigResource(ConfigResource.Type.BROKER, String.valueOf(BROKER_ID)),
                    singletonList(
                        new AlterConfigOp(
                            new ConfigEntry(CONFIG_1.getName(), /* value= */ null),
                            AlterConfigOp.OpType.DELETE)))))
        .andReturn(alterConfigsResult);
    expect(alterConfigsResult.values())
        .andReturn(
            singletonMap(
                new ConfigResource(ConfigResource.Type.BROKER, String.valueOf(BROKER_ID)),
                KafkaFuture.completedFuture(null)));
    replay(clusterManager, adminClient, describeConfigsResult, alterConfigsResult);

    brokerConfigManager.resetBrokerConfig(CLUSTER_ID, BROKER_ID, CONFIG_1.getName()).get();

    verify(adminClient);
  }

  @Test
  public void resetBrokerConfig_nonExistingConfig_throwsNotFound() throws Exception {
    expect(clusterManager.getCluster(CLUSTER_ID)).andReturn(completedFuture(Optional.of(CLUSTER)));
    expect(
            adminClient.describeConfigs(
                eq(
                    singletonList(
                        new ConfigResource(ConfigResource.Type.BROKER, String.valueOf(BROKER_ID)))),
                anyObject(DescribeConfigsOptions.class)))
        .andReturn(describeConfigsResult);
    expect(describeConfigsResult.all())
        .andReturn(
            KafkaFuture.completedFuture(
                singletonMap(
                    new ConfigResource(ConfigResource.Type.BROKER, String.valueOf(BROKER_ID)),
                    CONFIG)));
    replay(clusterManager, adminClient, describeConfigsResult);

    try {
      brokerConfigManager.resetBrokerConfig(CLUSTER_ID, BROKER_ID, "foobar").get();
      fail();
    } catch (ExecutionException e) {
      assertEquals(NotFoundException.class, e.getCause().getClass());
    }

    verify(adminClient);
  }

  @Test
  public void resetBrokerConfig_nonExistingBroker_throwsNotFound() throws Exception {
    expect(clusterManager.getCluster(CLUSTER_ID)).andReturn(completedFuture(Optional.of(CLUSTER)));
    expect(
            adminClient.describeConfigs(
                eq(
                    singletonList(
                        new ConfigResource(ConfigResource.Type.BROKER, String.valueOf(BROKER_ID)))),
                anyObject(DescribeConfigsOptions.class)))
        .andReturn(describeConfigsResult);
    expect(describeConfigsResult.all())
        .andReturn(KafkaFutures.failedFuture(new NotFoundException()));
    replay(clusterManager, adminClient, describeConfigsResult);

    try {
      brokerConfigManager.resetBrokerConfig(CLUSTER_ID, BROKER_ID, CONFIG_1.getName()).get();
      fail();
    } catch (ExecutionException e) {
      assertEquals(NotFoundException.class, e.getCause().getClass());
    }

    verify(adminClient);
  }

  @Test
  public void resetBrokerConfig_nonExistingCluster_throwsNotFound() throws Exception {
    expect(clusterManager.getCluster(CLUSTER_ID)).andReturn(completedFuture(Optional.empty()));
    replay(clusterManager, adminClient);

    try {
      brokerConfigManager.resetBrokerConfig(CLUSTER_ID, BROKER_ID, CONFIG_1.getName()).get();
      fail();
    } catch (ExecutionException e) {
      assertEquals(NotFoundException.class, e.getCause().getClass());
    }

    verify(adminClient);
  }

  @Test
  public void alterBrokerConfigs_existingConfigs_alterConfigs() throws Exception {
    expect(clusterManager.getCluster(CLUSTER_ID)).andReturn(completedFuture(Optional.of(CLUSTER)));
    expect(
            adminClient.describeConfigs(
                eq(
                    singletonList(
                        new ConfigResource(ConfigResource.Type.BROKER, String.valueOf(BROKER_ID)))),
                anyObject(DescribeConfigsOptions.class)))
        .andReturn(describeConfigsResult);
    expect(describeConfigsResult.all())
        .andReturn(
            KafkaFuture.completedFuture(
                singletonMap(
                    new ConfigResource(ConfigResource.Type.BROKER, String.valueOf(BROKER_ID)),
                    CONFIG)));
    expect(
            adminClient.incrementalAlterConfigs(
                singletonMap(
                    new ConfigResource(ConfigResource.Type.BROKER, String.valueOf(BROKER_ID)),
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
                new ConfigResource(ConfigResource.Type.BROKER, String.valueOf(BROKER_ID)),
                KafkaFuture.completedFuture(null)));
    replay(clusterManager, adminClient, describeConfigsResult, alterConfigsResult);

    brokerConfigManager
        .alterBrokerConfigs(
            CLUSTER_ID,
            BROKER_ID,
            Arrays.asList(
                AlterConfigCommand.set(CONFIG_1.getName(), "new-value"),
                AlterConfigCommand.delete(CONFIG_2.getName())))
        .get();

    verify(adminClient);
  }

  @Test
  public void alterBrokerConfigs_nonExistingCluster_throwsNotFound() throws Exception {
    expect(clusterManager.getCluster(CLUSTER_ID)).andReturn(completedFuture(Optional.empty()));
    replay(clusterManager);

    try {
      brokerConfigManager
          .alterBrokerConfigs(
              CLUSTER_ID,
              BROKER_ID,
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
  public void alterBrokerConfigs_oneNonExistingConfig_throwsNotFound() throws Exception {
    expect(clusterManager.getCluster(CLUSTER_ID)).andReturn(completedFuture(Optional.of(CLUSTER)));
    expect(
            adminClient.describeConfigs(
                eq(
                    singletonList(
                        new ConfigResource(ConfigResource.Type.BROKER, String.valueOf(BROKER_ID)))),
                anyObject(DescribeConfigsOptions.class)))
        .andReturn(describeConfigsResult);
    expect(describeConfigsResult.all())
        .andReturn(
            KafkaFuture.completedFuture(
                singletonMap(
                    new ConfigResource(ConfigResource.Type.BROKER, String.valueOf(BROKER_ID)),
                    CONFIG)));
    replay(clusterManager, adminClient, describeConfigsResult);

    try {
      brokerConfigManager
          .alterBrokerConfigs(
              CLUSTER_ID,
              BROKER_ID,
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
