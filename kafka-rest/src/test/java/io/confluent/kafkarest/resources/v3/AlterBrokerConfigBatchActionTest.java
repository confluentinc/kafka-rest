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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.confluent.kafkarest.Errors;
import io.confluent.kafkarest.controllers.BrokerConfigManager;
import io.confluent.kafkarest.entities.AlterConfigCommand;
import io.confluent.kafkarest.entities.BrokerConfig;
import io.confluent.kafkarest.entities.ConfigSource;
import io.confluent.kafkarest.entities.v3.AlterBrokerConfigBatchRequest;
import io.confluent.kafkarest.entities.v3.AlterConfigBatchRequestData;
import io.confluent.kafkarest.entities.v3.AlterConfigBatchRequestData.AlterEntry;
import io.confluent.kafkarest.entities.v3.AlterConfigBatchRequestData.AlterOperation;
import io.confluent.kafkarest.response.FakeAsyncResponse;
import io.confluent.rest.exceptions.RestConstraintViolationException;
import jakarta.ws.rs.NotFoundException;
import java.util.Arrays;
import org.easymock.EasyMockExtension;
import org.easymock.Mock;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(EasyMockExtension.class)
public final class AlterBrokerConfigBatchActionTest {

  private static final String CLUSTER_ID = "cluster-1";
  private static final int BROKER_ID = 1;

  private static final BrokerConfig CONFIG_1 =
      BrokerConfig.create(
          CLUSTER_ID,
          BROKER_ID,
          "config-1",
          "value-1",
          /* isDefault= */ true,
          /* isReadOnly= */ false,
          /* isSensitive */ false,
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
          /* isSensitive */ false,
          ConfigSource.STATIC_BROKER_CONFIG,
          /* synonyms= */ emptyList());

  @Mock private BrokerConfigManager brokerConfigManager;

  private AlterBrokerConfigBatchAction alterBrokerConfigBatchAction;

  @BeforeEach
  public void setUp() {
    alterBrokerConfigBatchAction = new AlterBrokerConfigBatchAction(() -> brokerConfigManager);
  }

  @Test
  public void alterBrokerConfigs_nullPayload() {
    RestConstraintViolationException e =
        assertThrows(
            RestConstraintViolationException.class,
            () ->
                alterBrokerConfigBatchAction.alterBrokerConfigBatch(
                    new FakeAsyncResponse(), CLUSTER_ID, BROKER_ID, null));
    assertTrue(e.getMessage().contains(Errors.NULL_PAYLOAD_ERROR_MESSAGE));
  }

  @Test
  public void alterBrokerConfigs_existingConfig_alterConfigs() {
    expect(
            brokerConfigManager.alterBrokerConfigs(
                CLUSTER_ID,
                BROKER_ID,
                Arrays.asList(
                    AlterConfigCommand.set(CONFIG_1.getName(), "newValue"),
                    AlterConfigCommand.delete(CONFIG_2.getName()))))
        .andReturn(completedFuture(null));
    replay(brokerConfigManager);

    FakeAsyncResponse response = new FakeAsyncResponse();
    alterBrokerConfigBatchAction.alterBrokerConfigBatch(
        response,
        CLUSTER_ID,
        BROKER_ID,
        AlterBrokerConfigBatchRequest.create(
            AlterConfigBatchRequestData.create(
                Arrays.asList(
                    AlterEntry.builder().setName(CONFIG_1.getName()).setValue("newValue").build(),
                    AlterEntry.builder()
                        .setName(CONFIG_2.getName())
                        .setOperation(AlterOperation.DELETE)
                        .build()))));

    assertNull(response.getValue());
    assertNull(response.getException());
    assertTrue(response.isDone());
  }

  @Test
  public void alterBrokerConfigs_nonExistingCluster_throwsNotFound() {
    expect(
            brokerConfigManager.alterBrokerConfigs(
                CLUSTER_ID,
                BROKER_ID,
                Arrays.asList(
                    AlterConfigCommand.set(CONFIG_1.getName(), "newValue"),
                    AlterConfigCommand.delete(CONFIG_2.getName()))))
        .andReturn(failedFuture(new NotFoundException()));
    replay(brokerConfigManager);

    FakeAsyncResponse response = new FakeAsyncResponse();
    alterBrokerConfigBatchAction.alterBrokerConfigBatch(
        response,
        CLUSTER_ID,
        BROKER_ID,
        AlterBrokerConfigBatchRequest.create(
            AlterConfigBatchRequestData.create(
                Arrays.asList(
                    AlterEntry.builder().setName(CONFIG_1.getName()).setValue("newValue").build(),
                    AlterEntry.builder()
                        .setName(CONFIG_2.getName())
                        .setOperation(AlterOperation.DELETE)
                        .build()))));

    assertEquals(NotFoundException.class, response.getException().getClass());
  }
}
