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
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.confluent.kafkarest.Errors;
import io.confluent.kafkarest.controllers.TopicConfigManager;
import io.confluent.kafkarest.entities.AlterConfigCommand;
import io.confluent.kafkarest.entities.v3.AlterConfigBatchRequestData.AlterEntry;
import io.confluent.kafkarest.entities.v3.AlterConfigBatchRequestData.AlterOperation;
import io.confluent.kafkarest.entities.v3.AlterMultipleTopicsConfigsBatchRequest;
import io.confluent.kafkarest.entities.v3.AlterMultipleTopicsConfigsBatchRequestData;
import io.confluent.kafkarest.entities.v3.AlterMultipleTopicsConfigsBatchRequestData.TopicAlterEntry;
import io.confluent.kafkarest.entities.v3.AlterMultipleTopicsConfigsBatchResponse;
import io.confluent.kafkarest.response.FakeAsyncResponse;
import io.confluent.rest.exceptions.RestConstraintViolationException;
import jakarta.ws.rs.NotFoundException;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.easymock.EasyMockExtension;
import org.easymock.Mock;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(EasyMockExtension.class)
public class AlterMultipleTopicsConfigsBatchActionTest {

  private static final String CLUSTER_ID = "cluster-1";
  private static final String TOPIC_1 = "topic-1";
  private static final String TOPIC_2 = "topic-2";

  @Mock private TopicConfigManager topicConfigManager;

  private AlterMultipleTopicsConfigsBatchAction action;

  @BeforeEach
  public void setUp() {
    action = new AlterMultipleTopicsConfigsBatchAction(() -> topicConfigManager);
  }

  @Test
  public void alterMultipleTopicsConfigsBatch_nullPayload_throwsConstraintViolation() {
    RestConstraintViolationException e =
        assertThrows(
            RestConstraintViolationException.class,
            () ->
                action.alterMultipleTopicsConfigsBatch(new FakeAsyncResponse(), CLUSTER_ID, null));
    assertTrue(e.getMessage().contains(Errors.NULL_PAYLOAD_ERROR_MESSAGE));
  }

  @Test
  public void alterMultipleTopicsConfigsBatch_existingTopics_returnsNoContent() {
    Map<String, List<AlterConfigCommand>> commandsByTopic = new LinkedHashMap<>();
    commandsByTopic.put(
        TOPIC_1,
        Arrays.asList(
            AlterConfigCommand.set("cleanup.policy", "compact"),
            AlterConfigCommand.delete("compression.type")));
    commandsByTopic.put(TOPIC_2, Arrays.asList(AlterConfigCommand.set("compression.type", "gzip")));

    expect(topicConfigManager.alterMultipleTopicsConfigs(CLUSTER_ID, commandsByTopic, false))
        .andReturn(completedFuture(Collections.emptyMap()));
    replay(topicConfigManager);

    FakeAsyncResponse response = new FakeAsyncResponse();
    action.alterMultipleTopicsConfigsBatch(
        response,
        CLUSTER_ID,
        AlterMultipleTopicsConfigsBatchRequest.create(
            AlterMultipleTopicsConfigsBatchRequestData.create(
                Arrays.asList(
                    TopicAlterEntry.create(
                        TOPIC_1,
                        Arrays.asList(
                            AlterEntry.builder()
                                .setName("cleanup.policy")
                                .setValue("compact")
                                .build(),
                            AlterEntry.builder()
                                .setName("compression.type")
                                .setOperation(AlterOperation.DELETE)
                                .build())),
                    TopicAlterEntry.create(
                        TOPIC_2,
                        Collections.singletonList(
                            AlterEntry.builder()
                                .setName("compression.type")
                                .setValue("gzip")
                                .build()))),
                Optional.empty())));

    // 204 No Content — entity is null
    assertNull(response.getValue());
    assertNull(response.getException());
    assertTrue(response.isDone());
  }

  @Test
  public void alterMultipleTopicsConfigsBatch_validateOnly_callsManagerWithValidateOnlyTrue() {
    Map<String, List<AlterConfigCommand>> commandsByTopic = new LinkedHashMap<>();
    commandsByTopic.put(
        TOPIC_1, Arrays.asList(AlterConfigCommand.set("cleanup.policy", "compact")));
    commandsByTopic.put(TOPIC_2, Arrays.asList(AlterConfigCommand.set("compression.type", "gzip")));

    expect(topicConfigManager.alterMultipleTopicsConfigs(CLUSTER_ID, commandsByTopic, true))
        .andReturn(completedFuture(Collections.emptyMap()));
    replay(topicConfigManager);

    FakeAsyncResponse response = new FakeAsyncResponse();
    action.alterMultipleTopicsConfigsBatch(
        response,
        CLUSTER_ID,
        AlterMultipleTopicsConfigsBatchRequest.create(
            AlterMultipleTopicsConfigsBatchRequestData.create(
                Arrays.asList(
                    TopicAlterEntry.create(
                        TOPIC_1,
                        Collections.singletonList(
                            AlterEntry.builder()
                                .setName("cleanup.policy")
                                .setValue("compact")
                                .build())),
                    TopicAlterEntry.create(
                        TOPIC_2,
                        Collections.singletonList(
                            AlterEntry.builder()
                                .setName("compression.type")
                                .setValue("gzip")
                                .build()))),
                Optional.of(true))));

    assertNull(response.getValue());
    assertNull(response.getException());
    assertTrue(response.isDone());
  }

  @Test
  public void alterMultipleTopicsConfigsBatch_nonExistingCluster_throwsNotFound() {
    Map<String, List<AlterConfigCommand>> commandsByTopic = new LinkedHashMap<>();
    commandsByTopic.put(
        TOPIC_1, Arrays.asList(AlterConfigCommand.set("cleanup.policy", "compact")));
    commandsByTopic.put(TOPIC_2, Arrays.asList(AlterConfigCommand.set("compression.type", "gzip")));

    expect(topicConfigManager.alterMultipleTopicsConfigs(CLUSTER_ID, commandsByTopic, false))
        .andReturn(failedFuture(new NotFoundException()));
    replay(topicConfigManager);

    FakeAsyncResponse response = new FakeAsyncResponse();
    action.alterMultipleTopicsConfigsBatch(
        response,
        CLUSTER_ID,
        AlterMultipleTopicsConfigsBatchRequest.create(
            AlterMultipleTopicsConfigsBatchRequestData.create(
                Arrays.asList(
                    TopicAlterEntry.create(
                        TOPIC_1,
                        Collections.singletonList(
                            AlterEntry.builder()
                                .setName("cleanup.policy")
                                .setValue("compact")
                                .build())),
                    TopicAlterEntry.create(
                        TOPIC_2,
                        Collections.singletonList(
                            AlterEntry.builder()
                                .setName("compression.type")
                                .setValue("gzip")
                                .build()))),
                Optional.empty())));

    assertEquals(NotFoundException.class, response.getException().getClass());
  }

  @Test
  public void alterMultipleTopicsConfigsBatch_partialFailure_returns207WithFailures() {
    Map<String, List<AlterConfigCommand>> commandsByTopic = new LinkedHashMap<>();
    commandsByTopic.put(
        TOPIC_1, Arrays.asList(AlterConfigCommand.set("cleanup.policy", "compact")));
    commandsByTopic.put(TOPIC_2, Arrays.asList(AlterConfigCommand.set("compression.type", "gzip")));

    Map<String, Throwable> failureMap = new LinkedHashMap<>();
    failureMap.put(TOPIC_2, new InvalidConfigurationException("bad value for topic-2"));

    expect(topicConfigManager.alterMultipleTopicsConfigs(CLUSTER_ID, commandsByTopic, false))
        .andReturn(completedFuture(failureMap));
    replay(topicConfigManager);

    FakeAsyncResponse response = new FakeAsyncResponse();
    action.alterMultipleTopicsConfigsBatch(
        response,
        CLUSTER_ID,
        AlterMultipleTopicsConfigsBatchRequest.create(
            AlterMultipleTopicsConfigsBatchRequestData.create(
                Arrays.asList(
                    TopicAlterEntry.create(
                        TOPIC_1,
                        Collections.singletonList(
                            AlterEntry.builder()
                                .setName("cleanup.policy")
                                .setValue("compact")
                                .build())),
                    TopicAlterEntry.create(
                        TOPIC_2,
                        Collections.singletonList(
                            AlterEntry.builder()
                                .setName("compression.type")
                                .setValue("gzip")
                                .build()))),
                Optional.empty())));

    // 207 Multi-Status — entity is the failure response
    assertNull(response.getException());
    assertTrue(response.isDone());
    AlterMultipleTopicsConfigsBatchResponse batchResponse =
        (AlterMultipleTopicsConfigsBatchResponse) response.getValue();
    assertEquals(1, batchResponse.getFailures().size());
    assertEquals(TOPIC_2, batchResponse.getFailures().get(0).getTopicName());
    assertTrue(batchResponse.getFailures().get(0).getMessage().isPresent());
    assertTrue(
        batchResponse.getFailures().get(0).getMessage().get().contains("bad value for topic-2"));
  }
}
