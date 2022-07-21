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
import io.confluent.kafkarest.entities.AlterConfigCommand;
import io.confluent.kafkarest.entities.ClusterConfig;
import io.confluent.kafkarest.entities.ConfigSource;
import io.confluent.kafkarest.entities.v3.AlterClusterConfigBatchRequest;
import io.confluent.kafkarest.entities.v3.AlterConfigBatchRequestData;
import io.confluent.kafkarest.entities.v3.AlterConfigBatchRequestData.AlterEntry;
import io.confluent.kafkarest.entities.v3.AlterConfigBatchRequestData.AlterOperation;
import io.confluent.kafkarest.response.FakeAsyncResponse;
import java.util.Arrays;
import javax.ws.rs.NotFoundException;
import org.easymock.EasyMockRule;
import org.easymock.Mock;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public final class AlterClusterConfigBatchActionTest {

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

  @Rule
  public final EasyMockRule mocks = new EasyMockRule(this);

  @Mock
  private ClusterConfigManager clusterConfigManager;

  private AlterClusterConfigBatchAction alterClusterConfigBatchAction;

  @Before
  public void setUp() {
    alterClusterConfigBatchAction = new AlterClusterConfigBatchAction(() -> clusterConfigManager);
  }

  @Test
  public void alterClusterConfigs_existingConfig_alterConfigs() {
    expect(
        clusterConfigManager.alterClusterConfigs(
            CLUSTER_ID,
            ClusterConfig.Type.BROKER,
            Arrays.asList(
                AlterConfigCommand.set(CONFIG_1.getName(), "newValue"),
                AlterConfigCommand.delete(CONFIG_2.getName()))))
        .andReturn(completedFuture(null));
    replay(clusterConfigManager);

    FakeAsyncResponse response = new FakeAsyncResponse();
    alterClusterConfigBatchAction.alterClusterConfigBatch(
        response,
        CLUSTER_ID,
        ClusterConfig.Type.BROKER,
        AlterClusterConfigBatchRequest.create(
            AlterConfigBatchRequestData.create(
                Arrays.asList(
                    AlterEntry.builder()
                        .setName(CONFIG_1.getName())
                        .setValue("newValue")
                        .build(),
                    AlterEntry.builder()
                        .setName(CONFIG_2.getName())
                        .setOperation(AlterOperation.DELETE)
                        .build()))));

    assertNull(response.getValue());
    assertNull(response.getException());
    assertTrue(response.isDone());
  }

  @Test
  public void alterClusterConfigs_nonExistingCluster_throwsNotFound() {
    expect(
        clusterConfigManager.alterClusterConfigs(
            CLUSTER_ID,
            ClusterConfig.Type.BROKER,
            Arrays.asList(
                AlterConfigCommand.set(CONFIG_1.getName(), "newValue"),
                AlterConfigCommand.delete(CONFIG_2.getName()))))
        .andReturn(failedFuture(new NotFoundException()));
    replay(clusterConfigManager);

    FakeAsyncResponse response = new FakeAsyncResponse();
    alterClusterConfigBatchAction.alterClusterConfigBatch(
        response,
        CLUSTER_ID,
        ClusterConfig.Type.BROKER,
        AlterClusterConfigBatchRequest.create(
            AlterConfigBatchRequestData.create(
                Arrays.asList(
                    AlterEntry.builder()
                        .setName(CONFIG_1.getName())
                        .setValue("newValue")
                        .build(),
                    AlterEntry.builder()
                        .setName(CONFIG_2.getName())
                        .setOperation(AlterOperation.DELETE)
                        .build()))));

    assertEquals(NotFoundException.class, response.getException().getClass());
  }
}
