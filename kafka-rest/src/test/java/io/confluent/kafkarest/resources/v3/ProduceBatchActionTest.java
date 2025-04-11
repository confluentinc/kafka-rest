/*
 * Copyright 2023 Confluent Inc.
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

import static io.confluent.kafkarest.KafkaRestConfig.PRODUCE_BATCH_MAXIMUM_ENTRIES_DEFAULT;
import static io.confluent.kafkarest.KafkaRestConfig.PRODUCE_MAX_BYTES_PER_SECOND;
import static io.confluent.kafkarest.KafkaRestConfig.PRODUCE_MAX_REQUESTS_PER_SECOND;
import static io.confluent.kafkarest.KafkaRestConfig.PRODUCE_RATE_LIMIT_CACHE_EXPIRY_MS;
import static io.confluent.kafkarest.KafkaRestConfig.PRODUCE_RATE_LIMIT_ENABLED;
import static java.util.Collections.emptyMap;
import static org.easymock.EasyMock.anyBoolean;
import static org.easymock.EasyMock.anyInt;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.replay;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.MoreExecutors;
import io.confluent.kafkarest.Errors;
import io.confluent.kafkarest.KafkaRestConfig;
import io.confluent.kafkarest.controllers.ProduceController;
import io.confluent.kafkarest.controllers.RecordSerializer;
import io.confluent.kafkarest.controllers.SchemaManager;
import io.confluent.kafkarest.entities.EmbeddedFormat;
import io.confluent.kafkarest.entities.ProduceResult;
import io.confluent.kafkarest.entities.v3.ProduceBatchRequest;
import io.confluent.kafkarest.entities.v3.ProduceBatchRequestEntry;
import io.confluent.kafkarest.entities.v3.ProduceBatchResponse;
import io.confluent.kafkarest.entities.v3.ProduceBatchResponseFailureEntry;
import io.confluent.kafkarest.entities.v3.ProduceBatchResponseSuccessEntry;
import io.confluent.kafkarest.entities.v3.ProduceRequest.ProduceRequestData;
import io.confluent.kafkarest.entities.v3.ProduceResponse.ProduceResponseData;
import io.confluent.kafkarest.exceptions.BadRequestException;
import io.confluent.kafkarest.ratelimit.RequestRateLimiter;
import io.confluent.kafkarest.response.FakeAsyncResponse;
import io.confluent.rest.exceptions.RestConstraintViolationException;
import java.lang.management.ManagementFactory;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import javax.inject.Provider;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import org.apache.kafka.common.metrics.Metrics;
import org.easymock.EasyMock;
import org.easymock.Mock;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ProduceBatchActionTest {

  @Mock private ProduceController produceController;

  private ProduceBatchAction produceBatchAction;
  private KafkaRestConfig kafkaRestConfig;

  @BeforeEach
  public void setUp() {
    Properties properties = new Properties();
    properties.put(PRODUCE_MAX_REQUESTS_PER_SECOND, Integer.toString(10000));
    properties.put(PRODUCE_MAX_BYTES_PER_SECOND, Integer.toString(999999999));
    properties.put(PRODUCE_RATE_LIMIT_ENABLED, "true");
    properties.put(PRODUCE_RATE_LIMIT_CACHE_EXPIRY_MS, Integer.toString(3600000));

    // setup
    Provider<RequestRateLimiter> countLimitProvider = mock(Provider.class);
    Provider<RequestRateLimiter> bytesLimitProvider = mock(Provider.class);
    Provider<RequestRateLimiter> countLimiterGlobalProvider = mock(Provider.class);
    Provider<RequestRateLimiter> bytesLimiterGlobalProvider = mock(Provider.class);
    RequestRateLimiter rateLimiterForCount = mock(RequestRateLimiter.class);
    RequestRateLimiter rateLimiterForBytes = mock(RequestRateLimiter.class);
    RequestRateLimiter countLimiterGlobal = mock(RequestRateLimiter.class);
    RequestRateLimiter bytesLimiterGlobal = mock(RequestRateLimiter.class);

    expect(countLimitProvider.get()).andReturn(rateLimiterForCount).anyTimes();
    expect(bytesLimitProvider.get()).andReturn(rateLimiterForBytes).anyTimes();
    expect(countLimiterGlobalProvider.get()).andReturn(countLimiterGlobal).anyTimes();
    expect(bytesLimiterGlobalProvider.get()).andReturn(bytesLimiterGlobal).anyTimes();
    rateLimiterForCount.rateLimit(anyInt());
    expectLastCall().andVoid().anyTimes();
    rateLimiterForBytes.rateLimit(anyInt());
    expectLastCall().andVoid().anyTimes();
    bytesLimiterGlobal.rateLimit(anyInt());
    expectLastCall().andVoid().anyTimes();
    countLimiterGlobal.rateLimit(anyInt());
    expectLastCall().andVoid().anyTimes();

    replay(
        countLimitProvider,
        bytesLimitProvider,
        rateLimiterForCount,
        rateLimiterForBytes,
        countLimiterGlobal,
        bytesLimiterGlobal,
        countLimiterGlobalProvider,
        bytesLimiterGlobalProvider);

    ProduceRateLimiters produceRateLimiters =
        new ProduceRateLimiters(
            countLimitProvider,
            bytesLimitProvider,
            countLimiterGlobalProvider,
            bytesLimiterGlobalProvider,
            Boolean.parseBoolean(properties.getProperty(PRODUCE_RATE_LIMIT_ENABLED)),
            Duration.ofMillis(
                Integer.parseInt(properties.getProperty(PRODUCE_RATE_LIMIT_CACHE_EXPIRY_MS))));

    Provider<SchemaManager> schemaManagerProvider = mock(Provider.class);
    SchemaManager schemaManagerMock = mock(SchemaManager.class);
    expect(schemaManagerProvider.get()).andReturn(schemaManagerMock).anyTimes();
    expect(
            schemaManagerMock.getSchema(
                "topicName",
                Optional.of(EmbeddedFormat.AVRO),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.of("bob"),
                true))
        .andThrow(
            Errors.invalidPayloadException(
                "Schema Registry must be configured when using schemas."));
    replay(schemaManagerProvider, schemaManagerMock);
    Provider<RecordSerializer> recordSerializerProvider = getRecordSerializerProvider(false);

    Provider<ProduceController> produceControllerProvider = mock(Provider.class);
    produceController = mock(ProduceController.class);
    expect(produceControllerProvider.get()).andReturn(produceController).anyTimes();
    replay(produceControllerProvider);

    // get the current thread so that the call counts can be seen by easy mock
    ExecutorService executorService = MoreExecutors.newDirectExecutorService();

    kafkaRestConfig = new KafkaRestConfig();
    kafkaRestConfig.setMetrics(new Metrics());

    produceBatchAction =
        new ProduceBatchAction(
            schemaManagerProvider,
            recordSerializerProvider,
            produceControllerProvider,
            () -> new ProducerMetrics(kafkaRestConfig, emptyMap()),
            produceRateLimiters,
            Integer.valueOf(PRODUCE_BATCH_MAXIMUM_ENTRIES_DEFAULT),
            executorService);
    produceRateLimiters.clear();
  }

  @AfterAll
  public static void cleanUp() {
    MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
    // Need to unregister the produce metrics bean to avoid affecting the metrics tests
    try {
      mBeanServer.unregisterMBean(new ObjectName("kafka.rest:type=produce-api-metrics"));
    } catch (MalformedObjectNameException
        | InstanceNotFoundException
        | MBeanRegistrationException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testProduceNoSchemaRegistryDefined() throws Exception {
    // produce controller returns an incomplete future
    CompletableFuture<ProduceResult> future = new CompletableFuture<>();
    future.completeExceptionally(
        new RestConstraintViolationException(
            "Payload error. Schema Registry must be configured when using schemas.", 42206));
    expect(
            produceController.produce(
                anyObject(),
                anyObject(),
                anyObject(),
                anyObject(),
                anyObject(),
                anyObject(),
                anyObject()))
        .andReturn(future);
    replay(produceController);

    ProduceRequestData key =
        ProduceRequestData.builder()
            .setFormat(EmbeddedFormat.AVRO)
            .setData(TextNode.valueOf("bob"))
            .setRawSchema("bob")
            .build();

    ProduceBatchRequestEntry entry =
        ProduceBatchRequestEntry.builder()
            .setId(new TextNode("1"))
            .setOriginalSize(25L)
            .setKey(key)
            .build();

    ProduceBatchRequest request =
        ProduceBatchRequest.builder().setEntries(Collections.singletonList(entry)).build();

    ProduceBatchResponseFailureEntry error =
        ProduceBatchResponseFailureEntry.builder()
            .setId("1")
            .setErrorCode(422)
            .setMessage(
                "Error: 42206 : Payload error. Schema Registry must be configured when "
                    + "using schemas.")
            .build();
    ProduceBatchResponse expected =
        ProduceBatchResponse.builder()
            .setSuccesses(Collections.emptyList())
            .setFailures(Collections.singletonList(error))
            .build();

    // run test
    FakeAsyncResponse response = new FakeAsyncResponse();
    produceBatchAction.produce(response, "clusterId", "topicName", request);

    // check results
    assertEquals(207, response.getStatus());
    assertEquals(expected, response.getValue());
    assertNull(response.getException());
  }

  @Test
  public void testProduceNegativePartitionId() throws Exception {
    // produce controller returns an incomplete future
    CompletableFuture<ProduceResult> future = new CompletableFuture<>();
    future.completeExceptionally(Errors.partitionNotFoundException());
    expect(
            produceController.produce(
                anyObject(),
                anyObject(),
                anyObject(),
                anyObject(),
                anyObject(),
                anyObject(),
                anyObject()))
        .andReturn(future);
    replay(produceController);

    ProduceRequestData key =
        ProduceRequestData.builder()
            .setFormat(EmbeddedFormat.AVRO)
            .setData(TextNode.valueOf("bob"))
            .setRawSchema("bob")
            .build();

    ProduceBatchRequestEntry entry =
        ProduceBatchRequestEntry.builder()
            .setId(new TextNode("1"))
            .setPartitionId(-1)
            .setOriginalSize(25L)
            .setKey(key)
            .build();

    ProduceBatchRequest request =
        ProduceBatchRequest.builder().setEntries(Collections.singletonList(entry)).build();

    ProduceBatchResponseFailureEntry error =
        ProduceBatchResponseFailureEntry.builder()
            .setId("1")
            .setErrorCode(40402)
            .setMessage(Errors.PARTITION_NOT_FOUND_MESSAGE)
            .build();
    ProduceBatchResponse expected =
        ProduceBatchResponse.builder()
            .setSuccesses(Collections.emptyList())
            .setFailures(Collections.singletonList(error))
            .build();

    // run test
    FakeAsyncResponse response = new FakeAsyncResponse();
    produceBatchAction.produce(response, "clusterId", "topicName", request);

    // check results
    assertEquals(207, response.getStatus());
    assertEquals(expected, response.getValue());
    assertNull(response.getException());
  }

  @Test
  public void testProduceBatch() throws Exception {
    final int numRecords = 4;

    for (int i = 0; i < numRecords; i++) {
      CompletableFuture<ProduceResult> future = new CompletableFuture<>();
      future.complete(ProduceResult.create(0, i, Instant.now(), 25, 0, Instant.now()));
      expect(
              produceController.produce(
                  anyObject(),
                  anyObject(),
                  anyObject(),
                  anyObject(),
                  anyObject(),
                  anyObject(),
                  anyObject()))
          .andReturn(future);
    }
    replay(produceController);

    ArrayList<ProduceBatchRequestEntry> entries = new ArrayList<>(numRecords);
    ArrayList<ProduceBatchResponseSuccessEntry> results = new ArrayList<>(numRecords);
    for (int i = 0; i < numRecords; i++) {
      ProduceRequestData key =
          ProduceRequestData.builder()
              .setFormat(EmbeddedFormat.STRING)
              .setData(TextNode.valueOf("bob"))
              .build();

      ProduceBatchRequestEntry entry =
          ProduceBatchRequestEntry.builder()
              .setId(new TextNode(Integer.toString(i)))
              .setOriginalSize(25L)
              .setKey(key)
              .build();

      entries.add(entry);

      ProduceBatchResponseSuccessEntry result =
          ProduceBatchResponseSuccessEntry.builder()
              .setId(Integer.toString(i))
              .setClusterId("clusterId")
              .setTopicName("topicName")
              .setPartitionId(0)
              .setOffset(Long.valueOf(i))
              .setKey(ProduceResponseData.builder().setSize(25).build())
              .build();

      results.add(result);
    }

    ProduceBatchRequest request = ProduceBatchRequest.builder().setEntries(entries).build();

    ProduceBatchResponse expected =
        ProduceBatchResponse.builder()
            .setSuccesses(results)
            .setFailures(Collections.emptyList())
            .build();

    // run test
    FakeAsyncResponse response = new FakeAsyncResponse();
    produceBatchAction.produce(response, "clusterId", "topicName", request);

    // check results
    assertEquals(207, response.getStatus());
    assertProduceBatchResponse(expected, (ProduceBatchResponse) (response.getValue()));
    assertNull(response.getException());
  }

  @Test
  public void testProduceBatchNonnumericIds() throws Exception {
    expect(
            produceController.produce(
                anyObject(),
                anyObject(),
                anyObject(),
                anyObject(),
                anyObject(),
                anyObject(),
                anyObject()))
        .andReturn(
            CompletableFuture.completedFuture(
                ProduceResult.create(0, 0, Instant.now(), 25, 0, Instant.now())))
        .andReturn(
            CompletableFuture.completedFuture(
                ProduceResult.create(0, 1, Instant.now(), 25, 0, Instant.now())));
    replay(produceController);

    List<ProduceBatchRequestEntry> entries =
        ImmutableList.of(
            ProduceBatchRequestEntry.builder()
                .setId(TextNode.valueOf("entry-1"))
                .setKey(
                    ProduceRequestData.builder()
                        .setFormat(EmbeddedFormat.STRING)
                        .setData(TextNode.valueOf("bob"))
                        .build())
                .setOriginalSize(25L)
                .build(),
            ProduceBatchRequestEntry.builder()
                .setId(TextNode.valueOf("0-9_a-z_A-Z"))
                .setKey(
                    ProduceRequestData.builder()
                        .setFormat(EmbeddedFormat.STRING)
                        .setData(TextNode.valueOf("bob"))
                        .build())
                .setOriginalSize(25L)
                .build());

    ProduceBatchRequest request = ProduceBatchRequest.builder().setEntries(entries).build();

    // run test
    FakeAsyncResponse response = new FakeAsyncResponse();
    produceBatchAction.produce(response, "clusterId", "topicName", request);

    ProduceBatchResponse expected =
        ProduceBatchResponse.builder()
            .setSuccesses(
                ImmutableList.of(
                    ProduceBatchResponseSuccessEntry.builder()
                        .setId("entry-1")
                        .setClusterId("clusterId")
                        .setTopicName("topicName")
                        .setPartitionId(0)
                        .setOffset(Long.valueOf(0))
                        .setKey(ProduceResponseData.builder().setSize(25).build())
                        .build(),
                    ProduceBatchResponseSuccessEntry.builder()
                        .setId("0-9_a-z_A-Z")
                        .setClusterId("clusterId")
                        .setTopicName("topicName")
                        .setPartitionId(0)
                        .setOffset(Long.valueOf(1))
                        .setKey(ProduceResponseData.builder().setSize(25).build())
                        .build()))
            .setFailures(Collections.emptyList())
            .build();

    // check results
    assertEquals(207, response.getStatus());
    assertProduceBatchResponse(expected, (ProduceBatchResponse) (response.getValue()));
    assertNull(response.getException());
  }

  @Test
  public void testProduceBatchNullRequest() throws Exception {
    // run test
    FakeAsyncResponse response = new FakeAsyncResponse();
    RestConstraintViolationException exception =
        assertThrows(
            RestConstraintViolationException.class,
            () -> produceBatchAction.produce(response, "clusterId", "topicName", null));

    // check results
    assertEquals(42208, exception.getErrorCode());
    assertEquals("Request body is empty. Data is required.", exception.getMessage());
    assertEquals(422, exception.getStatus());
  }

  @Test
  public void testProduceBatchEmptyRequest() throws Exception {
    ProduceBatchRequest request =
        ProduceBatchRequest.builder().setEntries(Collections.emptyList()).build();

    // run test
    FakeAsyncResponse response = new FakeAsyncResponse();
    RestConstraintViolationException exception =
        assertThrows(
            RestConstraintViolationException.class,
            () -> produceBatchAction.produce(response, "clusterId", "topicName", request));

    // check results
    assertEquals(42208, exception.getErrorCode());
    assertEquals("Empty batch.", exception.getMessage());
    assertEquals(422, exception.getStatus());
  }

  @Test
  public void testProduceBatchOversizeRequest() throws Exception {
    List<ProduceBatchRequestEntry> entries =
        ImmutableList.of(
            ProduceBatchRequestEntry.builder()
                .setId(TextNode.valueOf(Integer.toString(0)))
                .setKey(
                    ProduceRequestData.builder()
                        .setFormat(EmbeddedFormat.STRING)
                        .setData(TextNode.valueOf("bob"))
                        .build())
                .setOriginalSize(25L)
                .build(),
            ProduceBatchRequestEntry.builder()
                .setId(TextNode.valueOf(Integer.toString(1)))
                .setKey(
                    ProduceRequestData.builder()
                        .setFormat(EmbeddedFormat.STRING)
                        .setData(TextNode.valueOf("bob"))
                        .build())
                .setOriginalSize(25L)
                .build(),
            ProduceBatchRequestEntry.builder()
                .setId(TextNode.valueOf(Integer.toString(2)))
                .setKey(
                    ProduceRequestData.builder()
                        .setFormat(EmbeddedFormat.STRING)
                        .setData(TextNode.valueOf("bob"))
                        .build())
                .setOriginalSize(25L)
                .build(),
            ProduceBatchRequestEntry.builder()
                .setId(TextNode.valueOf(Integer.toString(3)))
                .setKey(
                    ProduceRequestData.builder()
                        .setFormat(EmbeddedFormat.STRING)
                        .setData(TextNode.valueOf("bob"))
                        .build())
                .setOriginalSize(25L)
                .build(),
            ProduceBatchRequestEntry.builder()
                .setId(TextNode.valueOf(Integer.toString(4)))
                .setKey(
                    ProduceRequestData.builder()
                        .setFormat(EmbeddedFormat.STRING)
                        .setData(TextNode.valueOf("bob"))
                        .build())
                .setOriginalSize(25L)
                .build(),
            ProduceBatchRequestEntry.builder()
                .setId(TextNode.valueOf(Integer.toString(5)))
                .setKey(
                    ProduceRequestData.builder()
                        .setFormat(EmbeddedFormat.STRING)
                        .setData(TextNode.valueOf("bob"))
                        .build())
                .setOriginalSize(25L)
                .build(),
            ProduceBatchRequestEntry.builder()
                .setId(TextNode.valueOf(Integer.toString(6)))
                .setKey(
                    ProduceRequestData.builder()
                        .setFormat(EmbeddedFormat.STRING)
                        .setData(TextNode.valueOf("bob"))
                        .build())
                .setOriginalSize(25L)
                .build(),
            ProduceBatchRequestEntry.builder()
                .setId(TextNode.valueOf(Integer.toString(7)))
                .setKey(
                    ProduceRequestData.builder()
                        .setFormat(EmbeddedFormat.STRING)
                        .setData(TextNode.valueOf("bob"))
                        .build())
                .setOriginalSize(25L)
                .build(),
            ProduceBatchRequestEntry.builder()
                .setId(TextNode.valueOf(Integer.toString(8)))
                .setKey(
                    ProduceRequestData.builder()
                        .setFormat(EmbeddedFormat.STRING)
                        .setData(TextNode.valueOf("bob"))
                        .build())
                .setOriginalSize(25L)
                .build(),
            ProduceBatchRequestEntry.builder()
                .setId(TextNode.valueOf(Integer.toString(9)))
                .setKey(
                    ProduceRequestData.builder()
                        .setFormat(EmbeddedFormat.STRING)
                        .setData(TextNode.valueOf("bob"))
                        .build())
                .setOriginalSize(25L)
                .build(),
            ProduceBatchRequestEntry.builder()
                .setId(TextNode.valueOf(Integer.toString(10)))
                .setKey(
                    ProduceRequestData.builder()
                        .setFormat(EmbeddedFormat.STRING)
                        .setData(TextNode.valueOf("bob"))
                        .build())
                .setOriginalSize(25L)
                .build());

    ProduceBatchRequest request = ProduceBatchRequest.builder().setEntries(entries).build();

    // run test
    FakeAsyncResponse response = new FakeAsyncResponse();
    RestConstraintViolationException exception =
        assertThrows(
            RestConstraintViolationException.class,
            () -> produceBatchAction.produce(response, "clusterId", "topicName", request));

    // check results
    assertEquals(42208, exception.getErrorCode());
    assertEquals("Too many entries in batch.", exception.getMessage());
    assertEquals(422, exception.getStatus());
  }

  @Test
  public void testProduceBatchWithDuplicateId() throws Exception {
    List<ProduceBatchRequestEntry> entries =
        ImmutableList.of(
            ProduceBatchRequestEntry.builder()
                .setId(TextNode.valueOf(Integer.toString(0)))
                .setKey(
                    ProduceRequestData.builder()
                        .setFormat(EmbeddedFormat.STRING)
                        .setData(TextNode.valueOf("bob"))
                        .build())
                .setOriginalSize(25L)
                .build(),
            ProduceBatchRequestEntry.builder()
                .setId(TextNode.valueOf(Integer.toString(0)))
                .setKey(
                    ProduceRequestData.builder()
                        .setFormat(EmbeddedFormat.STRING)
                        .setData(TextNode.valueOf("bob"))
                        .build())
                .setOriginalSize(25L)
                .build());

    ProduceBatchRequest request = ProduceBatchRequest.builder().setEntries(entries).build();

    // run test
    FakeAsyncResponse response = new FakeAsyncResponse();
    RestConstraintViolationException exception =
        assertThrows(
            RestConstraintViolationException.class,
            () -> produceBatchAction.produce(response, "clusterId", "topicName", request));

    // check results
    assertEquals(42208, exception.getErrorCode());
    assertEquals("Batch entry IDs are not distinct.", exception.getMessage());
    assertEquals(422, exception.getStatus());
  }

  @Test
  public void testProduceBatchFailed() throws Exception {
    final int numRecords = 4;

    for (int i = 0; i < numRecords; i++) {
      CompletableFuture<ProduceResult> future = new CompletableFuture<>();
      future.completeExceptionally(new BadRequestException("Invalid base64 string"));
      expect(
              produceController.produce(
                  anyObject(),
                  anyObject(),
                  anyObject(),
                  anyObject(),
                  anyObject(),
                  anyObject(),
                  anyObject()))
          .andReturn(future);
    }
    replay(produceController);

    ArrayList<ProduceBatchRequestEntry> entries = new ArrayList<>(numRecords);
    ArrayList<ProduceBatchResponseFailureEntry> results = new ArrayList<>(numRecords);
    for (int i = 0; i < numRecords; i++) {
      ProduceRequestData key =
          ProduceRequestData.builder()
              .setFormat(EmbeddedFormat.BINARY)
              .setData(TextNode.valueOf("fooba")) // invalid base64 string
              .build();

      ProduceBatchRequestEntry entry =
          ProduceBatchRequestEntry.builder()
              .setId(new TextNode(Integer.toString(i)))
              .setOriginalSize(25L)
              .setKey(key)
              .build();

      entries.add(entry);

      ProduceBatchResponseFailureEntry result =
          ProduceBatchResponseFailureEntry.builder()
              .setId(Integer.toString(i))
              .setErrorCode(400)
              .setMessage("Bad Request: Invalid base64 string")
              .build();

      results.add(result);
    }

    ProduceBatchRequest request = ProduceBatchRequest.builder().setEntries(entries).build();

    ProduceBatchResponse expected =
        ProduceBatchResponse.builder()
            .setSuccesses(Collections.emptyList())
            .setFailures(results)
            .build();

    // run test
    FakeAsyncResponse response = new FakeAsyncResponse();
    produceBatchAction.produce(response, "clusterId", "topicName", request);

    // check results
    assertEquals(207, response.getStatus());
    assertProduceBatchResponse(expected, (ProduceBatchResponse) (response.getValue()));
    assertNull(response.getException());
  }

  @Test
  public void testProduceBatchPartialFailure() throws Exception {
    CompletableFuture<ProduceResult> failedFuture = new CompletableFuture<>();
    failedFuture.completeExceptionally(new BadRequestException("Invalid base64 string"));
    expect(
            produceController.produce(
                anyObject(),
                anyObject(),
                anyObject(),
                anyObject(),
                anyObject(),
                anyObject(),
                anyObject()))
        .andReturn(
            CompletableFuture.completedFuture(
                ProduceResult.create(0, 0, Instant.now(), 25, 0, Instant.now())))
        .andReturn(failedFuture);
    replay(produceController);

    ProduceBatchRequest request =
        ProduceBatchRequest.builder()
            .setEntries(
                ImmutableList.of(
                    ProduceBatchRequestEntry.builder()
                        .setId(new TextNode("0"))
                        .setOriginalSize(25L)
                        .setKey(
                            ProduceRequestData.builder()
                                .setFormat(EmbeddedFormat.STRING)
                                .setData(TextNode.valueOf("bob"))
                                .build())
                        .build(),
                    ProduceBatchRequestEntry.builder()
                        .setId(new TextNode("1"))
                        .setOriginalSize(25L)
                        .setKey(
                            ProduceRequestData.builder()
                                .setFormat(EmbeddedFormat.BINARY)
                                .setData(TextNode.valueOf("fooba")) // invalid base64 string
                                .build())
                        .build()))
            .build();

    ProduceBatchResponse expected =
        ProduceBatchResponse.builder()
            .setSuccesses(
                Collections.singletonList(
                    ProduceBatchResponseSuccessEntry.builder()
                        .setId("0")
                        .setClusterId("clusterId")
                        .setTopicName("topicName")
                        .setPartitionId(0)
                        .setOffset(0L)
                        .setKey(ProduceResponseData.builder().setSize(25).build())
                        .build()))
            .setFailures(
                Collections.singletonList(
                    ProduceBatchResponseFailureEntry.builder()
                        .setId("1")
                        .setErrorCode(400)
                        .setMessage("Bad Request: Invalid base64 string")
                        .build()))
            .build();

    // run test
    FakeAsyncResponse response = new FakeAsyncResponse();
    produceBatchAction.produce(response, "clusterId", "topicName", request);

    // check results
    assertEquals(207, response.getStatus());
    assertProduceBatchResponse(expected, (ProduceBatchResponse) (response.getValue()));
    assertNull(response.getException());
  }

  @Test
  public void testProduceBatchNullIdFailure() throws Exception {
    // run test
    RestConstraintViolationException exception =
        assertThrows(
            RestConstraintViolationException.class,
            () ->
                ProduceBatchRequestEntry.builder()
                    .setId(NullNode.instance)
                    .setKey(
                        ProduceRequestData.builder()
                            .setFormat(EmbeddedFormat.STRING)
                            .setData(TextNode.valueOf("bob"))
                            .build())
                    .setOriginalSize(25L)
                    .build());

    // check results
    assertEquals(42208, exception.getErrorCode());
    assertEquals("Batch entry identifier is not a valid string.", exception.getMessage());
    assertEquals(422, exception.getStatus());
  }

  @Test
  public void testProduceBatchIntegerIdFailure() throws Exception {
    // run test
    RestConstraintViolationException exception =
        assertThrows(
            RestConstraintViolationException.class,
            () ->
                ProduceBatchRequestEntry.builder()
                    .setId(IntNode.valueOf(0))
                    .setKey(
                        ProduceRequestData.builder()
                            .setFormat(EmbeddedFormat.STRING)
                            .setData(TextNode.valueOf("bob"))
                            .build())
                    .setOriginalSize(25L)
                    .build());

    // check results
    assertEquals(42208, exception.getErrorCode());
    assertEquals("Batch entry identifier is not a valid string.", exception.getMessage());
    assertEquals(422, exception.getStatus());
  }

  @Test
  public void testProduceBatchEmptyStringIdFailure() throws Exception {
    // run test
    RestConstraintViolationException exception =
        assertThrows(
            RestConstraintViolationException.class,
            () ->
                ProduceBatchRequestEntry.builder()
                    .setId(TextNode.valueOf(""))
                    .setKey(
                        ProduceRequestData.builder()
                            .setFormat(EmbeddedFormat.STRING)
                            .setData(TextNode.valueOf("bob"))
                            .build())
                    .setOriginalSize(25L)
                    .build());

    // check results
    assertEquals(42208, exception.getErrorCode());
    assertEquals("Batch entry identifier is not a valid string.", exception.getMessage());
    assertEquals(422, exception.getStatus());
  }

  @Test
  public void testProduceBatchLongStringIdFailure() throws Exception {
    // run test
    RestConstraintViolationException exception =
        assertThrows(
            RestConstraintViolationException.class,
            () ->
                ProduceBatchRequestEntry.builder()
                    .setId(
                        TextNode.valueOf(
                            "0123456789012345678901234567890123456789"
                                + "01234567890123456789012345678901234567890"))
                    .setKey(
                        ProduceRequestData.builder()
                            .setFormat(EmbeddedFormat.STRING)
                            .setData(TextNode.valueOf("bob"))
                            .build())
                    .setOriginalSize(25L)
                    .build());

    // check results
    assertEquals(42208, exception.getErrorCode());
    assertEquals("Batch entry identifier is not a valid string.", exception.getMessage());
    assertEquals(422, exception.getStatus());
  }

  @Test
  public void testProduceBatchInvalidStringIdFailure() throws Exception {
    // run test
    RestConstraintViolationException exception =
        assertThrows(
            RestConstraintViolationException.class,
            () ->
                ProduceBatchRequestEntry.builder()
                    .setId(TextNode.valueOf("€#£"))
                    .setKey(
                        ProduceRequestData.builder()
                            .setFormat(EmbeddedFormat.STRING)
                            .setData(TextNode.valueOf("bob"))
                            .build())
                    .setOriginalSize(25L)
                    .build());

    // check results
    assertEquals(42208, exception.getErrorCode());
    assertEquals("Batch entry identifier is not a valid string.", exception.getMessage());
    assertEquals(422, exception.getStatus());
  }

  private static Provider<RecordSerializer> getRecordSerializerProvider(boolean error) {
    Provider<RecordSerializer> recordSerializerProvider = mock(Provider.class);
    RecordSerializer recordSerializer = mock(RecordSerializer.class);
    expect(recordSerializerProvider.get()).andReturn(recordSerializer).anyTimes();

    if (!error) {
      expect(
              recordSerializer.serialize(
                  anyObject(), anyObject(), anyObject(), anyObject(), anyBoolean()))
          .andReturn(Optional.empty())
          .anyTimes();
    } else {
      expect(
              recordSerializer.serialize(
                  anyObject(), anyObject(), anyObject(), anyObject(), anyBoolean()))
          .andThrow(
              Errors.messageSerializationException(
                  "Schema Registry not defined, "
                      + "no Schema Registry client available to deserialize message."))
          .anyTimes();
    }
    replay(recordSerializerProvider, recordSerializer);

    EasyMock.verify(recordSerializer);
    return recordSerializerProvider;
  }

  private void assertProduceBatchResponse(
      ProduceBatchResponse expected, ProduceBatchResponse actual) {
    int successesCount = actual.getSuccesses().size();
    assertEquals(expected.getSuccesses().size(), actual.getSuccesses().size());
    for (int i = 0; i < successesCount; i++) {
      ProduceBatchResponseSuccessEntry expectedSuccess = expected.getSuccesses().get(i);
      ProduceBatchResponseSuccessEntry actualSuccess = actual.getSuccesses().get(i);
      assertEquals(expectedSuccess.getId(), actualSuccess.getId());
      assertEquals(expectedSuccess.getClusterId(), actualSuccess.getClusterId());
      assertEquals(expectedSuccess.getTopicName(), actualSuccess.getTopicName());
      assertEquals(expectedSuccess.getPartitionId(), actualSuccess.getPartitionId());
      assertEquals(expectedSuccess.getOffset(), actualSuccess.getOffset());
    }

    int failuresCount = actual.getFailures().size();
    assertEquals(expected.getFailures().size(), actual.getFailures().size());
    for (int i = 0; i < failuresCount; i++) {
      ProduceBatchResponseFailureEntry expectedFailure = expected.getFailures().get(i);
      ProduceBatchResponseFailureEntry actualFailure = actual.getFailures().get(i);
      assertEquals(expectedFailure.getId(), actualFailure.getId());
      assertEquals(expectedFailure.getErrorCode(), actualFailure.getErrorCode());
      assertEquals(expectedFailure.getMessage(), actualFailure.getMessage());
    }
  }
}
