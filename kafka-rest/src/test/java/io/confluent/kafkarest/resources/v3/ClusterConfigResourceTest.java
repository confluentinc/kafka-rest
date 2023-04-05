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

import io.confluent.kafkarest.controllers.ClusterConfigManager;
import io.confluent.kafkarest.entities.ClusterConfig;
import io.confluent.kafkarest.entities.ConfigSource;
import io.confluent.kafkarest.entities.v3.ClusterConfigData;
import io.confluent.kafkarest.entities.v3.ClusterConfigDataList;
import io.confluent.kafkarest.entities.v3.ConfigSynonymData;
import io.confluent.kafkarest.entities.v3.GetClusterConfigResponse;
import io.confluent.kafkarest.entities.v3.ListClusterConfigsResponse;
import io.confluent.kafkarest.entities.v3.Resource;
import io.confluent.kafkarest.entities.v3.ResourceCollection;
import io.confluent.kafkarest.entities.v3.UpdateClusterConfigRequest;
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
public final class ClusterConfigResourceTest {

  private static final String CLUSTER_ID = "cluster-1";

  private static final ClusterConfig CONFIG_1 =
      ClusterConfig.create(
          CLUSTER_ID,
          "config-1",
          "value-1",
          /* isDefault= */ true,
          /* isReadOnly= */ false,
          /* isSensitive */ false,
          ConfigSource.DEFAULT_CONFIG,
          /* synonyms= */ emptyList(),
          ClusterConfig.Type.BROKER);
  private static final ClusterConfig CONFIG_2 =
      ClusterConfig.create(
          CLUSTER_ID,
          "config-2",
          "value-2",
          /* isDefault= */ false,
          /* isReadOnly= */ true,
          /* isSensitive */ false,
          ConfigSource.STATIC_BROKER_CONFIG,
          /* synonyms= */ emptyList(),
          ClusterConfig.Type.BROKER);
  private static final ClusterConfig CONFIG_3 =
      ClusterConfig.create(
          CLUSTER_ID,
          "config-3",
          "value-3",
          /* isDefault= */ false,
          /* isReadOnly= */ false,
          /* isSensitive */ true,
          ConfigSource.DYNAMIC_BROKER_CONFIG,
          /* synonyms= */ emptyList(),
          ClusterConfig.Type.BROKER);

  @Rule
  public final EasyMockRule mocks = new EasyMockRule(this);

  @Mock
  private ClusterConfigManager clusterConfigManager;

  private ClusterConfigsResource clusterConfigsResource;

  @Before
  public void setUp() {
    clusterConfigsResource =
        new ClusterConfigsResource(
            () -> clusterConfigManager,
            new CrnFactoryImpl(/* crnAuthorityConfig= */""),
            new FakeUrlFactory());
  }

  @Test
  public void listClusterConfigs_existingCluster_returnsConfigs() {
    expect(clusterConfigManager.listClusterConfigs(CLUSTER_ID, ClusterConfig.Type.BROKER))
        .andReturn(
            completedFuture(Arrays.asList(CONFIG_1, CONFIG_2, CONFIG_3)));
    replay(clusterConfigManager);

    FakeAsyncResponse response = new FakeAsyncResponse();
    clusterConfigsResource.listClusterConfigs(response, CLUSTER_ID, ClusterConfig.Type.BROKER);

    ListClusterConfigsResponse expected =
        ListClusterConfigsResponse.create(
            ClusterConfigDataList.builder()
                .setMetadata(
                    ResourceCollection.Metadata.builder()
                        .setSelf(
                            "/v3/clusters/cluster-1/broker-configs")
                        .build())
                .setData(
                    Arrays.asList(
                        ClusterConfigData.builder()
                            .setMetadata(
                                Resource.Metadata.builder()
                                    .setSelf(
                                        "/v3/clusters/cluster-1/broker-configs/config-1")
                                    .setResourceName(
                                        "crn:///kafka=cluster-1/broker-config=config-1")
                                    .build())
                            .setClusterId(CLUSTER_ID)
                            .setConfigType(ClusterConfig.Type.BROKER)
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
                        ClusterConfigData.builder()
                            .setMetadata(
                                Resource.Metadata.builder()
                                    .setSelf(
                                        "/v3/clusters/cluster-1/broker-configs/config-2")
                                    .setResourceName(
                                        "crn:///kafka=cluster-1/broker-config=config-2")
                                    .build())
                            .setClusterId(CLUSTER_ID)
                            .setConfigType(ClusterConfig.Type.BROKER)
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
                        ClusterConfigData.builder()
                            .setMetadata(
                                Resource.Metadata.builder()
                                    .setSelf(
                                        "/v3/clusters/cluster-1/broker-configs/config-3")
                                    .setResourceName(
                                        "crn:///kafka=cluster-1/broker-config=config-3")
                                    .build())
                            .setClusterId(CLUSTER_ID)
                            .setConfigType(ClusterConfig.Type.BROKER)
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
  public void listClusterConfigs_nonExistingCluster_throwsNotFound() {
    expect(clusterConfigManager.listClusterConfigs(CLUSTER_ID, ClusterConfig.Type.BROKER))
        .andReturn(failedFuture(new NotFoundException()));
    replay(clusterConfigManager);

    FakeAsyncResponse response = new FakeAsyncResponse();
    clusterConfigsResource.listClusterConfigs(response, CLUSTER_ID, ClusterConfig.Type.BROKER);

    assertEquals(NotFoundException.class, response.getException().getClass());
  }

  @Test
  public void getClusterConfig_existingConfig_returnsConfig() {
    expect(clusterConfigManager.getClusterConfig(
        CLUSTER_ID, ClusterConfig.Type.BROKER, CONFIG_1.getName()))
        .andReturn(completedFuture(Optional.of(CONFIG_1)));
    replay(clusterConfigManager);

    FakeAsyncResponse response = new FakeAsyncResponse();
    clusterConfigsResource.getClusterConfig(
        response, CLUSTER_ID, ClusterConfig.Type.BROKER, CONFIG_1.getName());
    GetClusterConfigResponse expected =
        GetClusterConfigResponse.create(
            ClusterConfigData.builder()
                .setMetadata(
                    Resource.Metadata.builder()
                        .setSelf(
                            "/v3/clusters/cluster-1/broker-configs/config-1")
                        .setResourceName(
                            "crn:///kafka=cluster-1/broker-config=config-1")
                        .build())
                .setClusterId(CLUSTER_ID)
                .setConfigType(ClusterConfig.Type.BROKER)
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
  public void getClusterConfig_nonExistingConfig_throwsNotFound() {
    expect(clusterConfigManager.getClusterConfig(
        CLUSTER_ID, ClusterConfig.Type.BROKER, CONFIG_1.getName()))
        .andReturn(failedFuture(new NotFoundException()));
    replay(clusterConfigManager);

    FakeAsyncResponse response = new FakeAsyncResponse();
    clusterConfigsResource.getClusterConfig(
        response, CLUSTER_ID, ClusterConfig.Type.BROKER, CONFIG_1.getName());

    assertEquals(NotFoundException.class, response.getException().getClass());
  }

  @Test
  public void getClusterConfig_nonExistingCluster_throwsNotFound() {
    expect(clusterConfigManager.getClusterConfig(
        CLUSTER_ID, ClusterConfig.Type.BROKER, CONFIG_1.getName()))
        .andReturn(failedFuture(new NotFoundException()));
    replay(clusterConfigManager);

    FakeAsyncResponse response = new FakeAsyncResponse();
    clusterConfigsResource.getClusterConfig(
        response, CLUSTER_ID, ClusterConfig.Type.BROKER, CONFIG_1.getName());

    assertEquals(NotFoundException.class, response.getException().getClass());
  }

  @Test
  public void updateClusterConfig_existingConfig_updatesConfig() {
    expect(
        clusterConfigManager.upsertClusterConfig(
            CLUSTER_ID,
            ClusterConfig.Type.BROKER,
            CONFIG_1.getName(),
            "new-value"))
        .andReturn(completedFuture(null));
    replay(clusterConfigManager);

    FakeAsyncResponse response = new FakeAsyncResponse();
    clusterConfigsResource.upsertClusterConfig(
        response,
        CLUSTER_ID,
        ClusterConfig.Type.BROKER,
        CONFIG_1.getName(),
        UpdateClusterConfigRequest.create("new-value"));
    assertNull(response.getValue());
    assertNull(response.getException());
    assertTrue(response.isDone());
  }

  @Test
  public void updateConfig_nonExistingCluster_throwsNotFound() {
    expect(
        clusterConfigManager.upsertClusterConfig(
            CLUSTER_ID,
            ClusterConfig.Type.BROKER,
            CONFIG_1.getName(),
            "new-value"))
        .andReturn(failedFuture(new NotFoundException()));
    replay(clusterConfigManager);

    FakeAsyncResponse response = new FakeAsyncResponse();
    clusterConfigsResource.upsertClusterConfig(
        response,
        CLUSTER_ID,
        ClusterConfig.Type.BROKER,
        CONFIG_1.getName(),
        UpdateClusterConfigRequest.create("new-value"));

    assertEquals(NotFoundException.class, response.getException().getClass());
  }

  @Test
  public void resetClusterConfig_existingConfig_resetsConfig() {
    expect(
        clusterConfigManager.deleteClusterConfig(
            CLUSTER_ID,
            ClusterConfig.Type.BROKER,
            CONFIG_1.getName()))
        .andReturn(completedFuture(null));
    replay(clusterConfigManager);

    FakeAsyncResponse response = new FakeAsyncResponse();
    clusterConfigsResource.deleteClusterConfig(
        response, CLUSTER_ID, ClusterConfig.Type.BROKER, CONFIG_1.getName());

    assertNull(response.getValue());
    assertNull(response.getException());
    assertTrue(response.isDone());
  }

  @Test
  public void resetClusterConfig_nonExistingCluster_throwsNotFound() {
    expect(
        clusterConfigManager.deleteClusterConfig(
            CLUSTER_ID,
            ClusterConfig.Type.BROKER,
            CONFIG_1.getName()))
        .andReturn(failedFuture(new NotFoundException()));
    replay(clusterConfigManager);

    FakeAsyncResponse response = new FakeAsyncResponse();
    clusterConfigsResource.deleteClusterConfig(
        response, CLUSTER_ID, ClusterConfig.Type.BROKER, CONFIG_1.getName());

    assertEquals(NotFoundException.class, response.getException().getClass());
  }
}
