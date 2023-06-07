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

import static java.util.Objects.requireNonNull;

import com.fasterxml.jackson.databind.node.NullNode;
import com.google.common.collect.ImmutableMultimap;
import com.google.protobuf.ByteString;
import io.confluent.kafkarest.Errors;
import io.confluent.kafkarest.config.ConfigModule.ProduceBatchMaximumEntriesConfig;
import io.confluent.kafkarest.controllers.ProduceController;
import io.confluent.kafkarest.controllers.RecordSerializer;
import io.confluent.kafkarest.controllers.SchemaManager;
import io.confluent.kafkarest.entities.EmbeddedFormat;
import io.confluent.kafkarest.entities.ProduceResult;
import io.confluent.kafkarest.entities.RegisteredSchema;
import io.confluent.kafkarest.entities.v3.ProduceBatchRequest;
import io.confluent.kafkarest.entities.v3.ProduceBatchRequestEntry;
import io.confluent.kafkarest.entities.v3.ProduceBatchResponse;
import io.confluent.kafkarest.entities.v3.ProduceBatchResponseFailureEntry;
import io.confluent.kafkarest.entities.v3.ProduceBatchResponseSuccessEntry;
import io.confluent.kafkarest.entities.v3.ProduceRequest.ProduceRequestData;
import io.confluent.kafkarest.entities.v3.ProduceRequest.ProduceRequestHeader;
import io.confluent.kafkarest.entities.v3.ProduceResponse.ProduceResponseData;
import io.confluent.kafkarest.exceptions.BadRequestException;
import io.confluent.kafkarest.exceptions.StacklessCompletionException;
import io.confluent.kafkarest.exceptions.v3.ErrorResponse;
import io.confluent.kafkarest.extension.ResourceAccesslistFeature.ResourceName;
import io.confluent.kafkarest.ratelimit.DoNotRateLimit;
import io.confluent.kafkarest.ratelimit.RateLimitExceededException;
import io.confluent.kafkarest.resources.v3.V3ResourcesModule.ProduceResponseThreadPool;
import io.confluent.kafkarest.response.StreamingResponse;
import io.confluent.rest.annotations.PerformanceMetric;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collector;
import javax.inject.Inject;
import javax.inject.Provider;
import javax.servlet.http.HttpServletRequest;
import javax.validation.Valid;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.Providers;
import org.apache.kafka.common.errors.SerializationException;

@DoNotRateLimit
@Path("/v3/clusters/{clusterId}/topics/{topicName}/records:batch")
@ResourceName("api.v3.batch-produce.*")
public final class ProduceBatchAction {

  public static final int BATCH_ID_MINIMUM_LENGTH = 1;
  public static final int BATCH_ID_MAXIMUM_LENGTH = 80;

  @Context private Providers providers;

  private static final Collector<
          ProduceRequestHeader,
          ImmutableMultimap.Builder<String, Optional<ByteString>>,
          ImmutableMultimap<String, Optional<ByteString>>>
      PRODUCE_REQUEST_HEADER_COLLECTOR =
          Collector.of(
              ImmutableMultimap::builder,
              (builder, header) -> builder.put(header.getName(), header.getValue()),
              (left, right) -> left.putAll(right.build()),
              ImmutableMultimap.Builder::build);

  private final Provider<SchemaManager> schemaManagerProvider;
  private final Provider<RecordSerializer> recordSerializerProvider;
  private final Provider<ProduceController> produceControllerProvider;
  private final Provider<ProducerMetrics> producerMetricsProvider;
  private final ProduceRateLimiters produceRateLimiters;
  private final int produceBatchMaximumEntries;
  private final ExecutorService executorService;

  @Context private HttpServletRequest httpServletRequest;

  @Inject
  public ProduceBatchAction(
      Provider<SchemaManager> schemaManagerProvider,
      Provider<RecordSerializer> recordSerializer,
      Provider<ProduceController> produceControllerProvider,
      Provider<ProducerMetrics> producerMetrics,
      ProduceRateLimiters produceRateLimiters,
      @ProduceBatchMaximumEntriesConfig Integer produceBatchMaximumEntries,
      @ProduceResponseThreadPool ExecutorService executorService) {
    this.schemaManagerProvider = requireNonNull(schemaManagerProvider);
    this.recordSerializerProvider = requireNonNull(recordSerializer);
    this.produceControllerProvider = requireNonNull(produceControllerProvider);
    this.producerMetricsProvider = requireNonNull(producerMetrics);
    this.produceRateLimiters = requireNonNull(produceRateLimiters);
    this.produceBatchMaximumEntries = requireNonNull(produceBatchMaximumEntries);
    this.executorService = requireNonNull(executorService);
  }

  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @PerformanceMetric("v3.produce.produce-to-topic")
  @ResourceName("api.v3.batch-produce.produce-to-topic")
  public void produce(
      @Suspended AsyncResponse asyncResponse,
      @PathParam("clusterId") String clusterId,
      @PathParam("topicName") String topicName,
      @Valid ProduceBatchRequest request)
      throws Exception {

    if (request == null) {
      throw Errors.produceBatchException(Errors.PRODUCE_BATCH_EXCEPTION_NULL_MESSAGE);
    }

    // First, pre-process the batch ensuring it's non-empty, not too large and the entry
    // IDs are distinct
    HashSet<String> entrySet = new HashSet<>();
    request.getEntries().stream()
        .forEach(
            (e) -> {
              if (!entrySet.add(e.getId().textValue())) {
                throw Errors.produceBatchException(
                    Errors.PRODUCE_BATCH_EXCEPTION_IDS_NOT_DISTINCT_MESSAGE);
              }
            });

    final int batchSize = request.getEntries().size();
    if (batchSize == 0) {
      throw Errors.produceBatchException(Errors.PRODUCE_BATCH_EXCEPTION_EMPTY_BATCH_MESSAGE);
    }

    if (batchSize > produceBatchMaximumEntries) {
      throw Errors.produceBatchException(Errors.PRODUCE_BATCH_EXCEPTION_TOO_MANY_ENTRIES_MESSAGE);
    }

    List<ProduceBatchResponseSuccessEntry> successes = new ArrayList<>(batchSize);
    List<ProduceBatchResponseFailureEntry> failures = new ArrayList<>(batchSize);

    ProduceController controller = produceControllerProvider.get();
    List<CompletableFuture<ProduceBatchResponseSuccessEntry>> responseFutures =
        new ArrayList<>(batchSize);

    for (ProduceBatchRequestEntry e : request.getEntries()) {
      responseFutures.add(
          produce(clusterId, topicName, e, controller, producerMetricsProvider.get()));
    }
    CompletableFuture.allOf(responseFutures.toArray(new CompletableFuture[0]))
        .whenCompleteAsync(
            (result, error) -> {
              for (int i = 0; i < batchSize; i++) {
                CompletableFuture<ProduceBatchResponseSuccessEntry> future = responseFutures.get(i);
                final String entryId = request.getEntries().get(i).getId().textValue();
                future.handle(
                    (responseResult, responseError) -> {
                      if (responseError != null) {
                        failures.add(toErrorEntryForException(entryId, responseError));
                      } else {
                        successes.add(responseResult);
                      }
                      return null;
                    });
              }
              asyncResponse.resume(
                  Response.status(207, "Multi-Status")
                      .entity(
                          ProduceBatchResponse.builder()
                              .setSuccesses(successes)
                              .setFailures(failures)
                              .build())
                      .build());
            },
            executorService);
  }

  private CompletableFuture<ProduceBatchResponseSuccessEntry> produce(
      String clusterId,
      String topicName,
      ProduceBatchRequestEntry request,
      ProduceController controller,
      ProducerMetrics metrics) {
    final long requestStartNs = System.nanoTime();

    try {
      try {
        produceRateLimiters.rateLimit(clusterId, request.getOriginalSize(), httpServletRequest);
      } catch (RateLimitExceededException e) {
        recordRateLimitedMetrics(metrics);
        // KREST-4356 Use our own CompletionException that will avoid the costly stack trace fill.
        throw new StacklessCompletionException(e);
      }

      // Request metrics are recorded before we check the validity of the message body, but after
      // rate limiting, as these metrics are used for billing.
      recordRequestMetrics(metrics, request.getOriginalSize());

      request
          .getPartitionId()
          .ifPresent(
              (partitionId) -> {
                if (partitionId < 0) {
                  throw Errors.partitionNotFoundException();
                }
              });

      Optional<RegisteredSchema> keySchema =
          request.getKey().flatMap(key -> getSchema(topicName, /* isKey= */ true, key));
      Optional<EmbeddedFormat> keyFormat =
          keySchema
              .map(schema -> Optional.of(schema.getFormat()))
              .orElse(request.getKey().flatMap(ProduceRequestData::getFormat));
      Optional<ByteString> serializedKey =
          serialize(topicName, keyFormat, keySchema, request.getKey(), /* isKey= */ true);

      Optional<RegisteredSchema> valueSchema =
          request.getValue().flatMap(value -> getSchema(topicName, /* isKey= */ false, value));
      Optional<EmbeddedFormat> valueFormat =
          valueSchema
              .map(schema -> Optional.of(schema.getFormat()))
              .orElse(request.getValue().flatMap(ProduceRequestData::getFormat));
      Optional<ByteString> serializedValue =
          serialize(topicName, valueFormat, valueSchema, request.getValue(), /* isKey= */ false);

      CompletableFuture<ProduceResult> produceResult =
          controller.produce(
              clusterId,
              topicName,
              request.getPartitionId(),
              request.getHeaders().stream().collect(PRODUCE_REQUEST_HEADER_COLLECTOR),
              serializedKey,
              serializedValue,
              request.getTimestamp().orElse(Instant.now()));

      return produceResult
          .handleAsync(
              (result, error) -> {
                if (error != null) {
                  long latency = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - requestStartNs);
                  recordErrorMetrics(metrics, latency);
                  throw new StacklessCompletionException(error);
                }
                return result;
              },
              executorService)
          .thenApplyAsync(
              result -> {
                ProduceBatchResponseSuccessEntry batchResult =
                    toResponseSuccessEntry(
                        request.getId().textValue(),
                        clusterId,
                        topicName,
                        keyFormat,
                        keySchema,
                        valueFormat,
                        valueSchema,
                        result);
                long latency = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - requestStartNs);
                recordResponseMetrics(metrics, latency);
                return batchResult;
              },
              executorService);
    } catch (Throwable t) {
      CompletableFuture<ProduceBatchResponseSuccessEntry> failedResult = new CompletableFuture<>();
      failedResult.completeExceptionally(t);
      return failedResult;
    }
  }

  private Optional<RegisteredSchema> getSchema(
      String topicName, boolean isKey, ProduceRequestData data) {
    if (data.getFormat().isPresent() && !data.getFormat().get().requiresSchema()) {
      return Optional.empty();
    }

    try {
      return Optional.of(
          schemaManagerProvider
              .get()
              .getSchema(
                  topicName,
                  data.getFormat(),
                  data.getSubject(),
                  data.getSubjectNameStrategy().map(Function.identity()),
                  data.getSchemaId(),
                  data.getSchemaVersion(),
                  data.getRawSchema(),
                  isKey));
    } catch (SerializationException se) {
      throw Errors.messageSerializationException(se.getMessage());
    } catch (IllegalArgumentException iae) {
      throw new BadRequestException(iae.getMessage(), iae);
    }
  }

  private Optional<ByteString> serialize(
      String topicName,
      Optional<EmbeddedFormat> format,
      Optional<RegisteredSchema> schema,
      Optional<ProduceRequestData> data,
      boolean isKey) {
    return recordSerializerProvider
        .get()
        .serialize(
            format.orElse(EmbeddedFormat.BINARY),
            topicName,
            schema,
            data.map(ProduceRequestData::getData).orElse(NullNode.getInstance()),
            isKey);
  }

  private static ProduceBatchResponseSuccessEntry toResponseSuccessEntry(
      String id,
      String clusterId,
      String topicName,
      Optional<EmbeddedFormat> keyFormat,
      Optional<RegisteredSchema> keySchema,
      Optional<EmbeddedFormat> valueFormat,
      Optional<RegisteredSchema> valueSchema,
      ProduceResult result) {
    return ProduceBatchResponseSuccessEntry.builder()
        .setId(id)
        .setClusterId(clusterId)
        .setTopicName(topicName)
        .setPartitionId(result.getPartitionId())
        .setOffset(result.getOffset())
        .setTimestamp(result.getTimestamp())
        .setKey(
            keyFormat.map(
                format ->
                    ProduceResponseData.builder()
                        .setType(keyFormat)
                        .setSubject(keySchema.map(RegisteredSchema::getSubject))
                        .setSchemaId(keySchema.map(RegisteredSchema::getSchemaId))
                        .setSchemaVersion(keySchema.map(RegisteredSchema::getSchemaVersion))
                        .setSize(result.getSerializedKeySize())
                        .build()))
        .setValue(
            valueFormat.map(
                format ->
                    ProduceResponseData.builder()
                        .setType(valueFormat)
                        .setSubject(valueSchema.map(RegisteredSchema::getSubject))
                        .setSchemaId(valueSchema.map(RegisteredSchema::getSchemaId))
                        .setSchemaVersion(valueSchema.map(RegisteredSchema::getSchemaVersion))
                        .setSize(result.getSerializedValueSize())
                        .build()))
        .build();
  }

  private ProduceBatchResponseFailureEntry toErrorEntryForException(String id, Throwable t) {
    ErrorResponse errorResponse;
    if (t instanceof CompletionException) {
      errorResponse = StreamingResponse.toErrorResponse(((CompletionException) t).getCause());
    } else {
      errorResponse = StreamingResponse.toErrorResponse(t);
    }

    ProduceBatchResponseFailureEntry.Builder builder = ProduceBatchResponseFailureEntry.builder();
    builder.setId(id);
    builder.setErrorCode(errorResponse.getErrorCode());
    builder.setMessage(errorResponse.getMessage());
    return builder.build();
  }

  private void recordResponseMetrics(ProducerMetrics metrics, long latencyMs) {
    metrics.recordResponse();
    metrics.recordRequestLatency(latencyMs);
  }

  private void recordErrorMetrics(ProducerMetrics metrics, long latencyMs) {
    metrics.recordError();
    metrics.recordRequestLatency(latencyMs);
  }

  private void recordRateLimitedMetrics(ProducerMetrics metrics) {
    metrics.recordRateLimited();
  }

  private void recordRequestMetrics(ProducerMetrics metrics, long size) {
    metrics.recordRequest();
    // record request size
    metrics.recordRequestSize(size);
  }
}
