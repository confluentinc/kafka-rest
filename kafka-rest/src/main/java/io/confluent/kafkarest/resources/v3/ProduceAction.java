/*
 * Copyright 2021 Confluent Inc.
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

import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.node.NullNode;
import com.google.common.collect.ImmutableMultimap;
import com.google.protobuf.ByteString;
import io.confluent.kafkarest.KafkaRestConfig;
import io.confluent.kafkarest.ProducerMetrics;
import io.confluent.kafkarest.ProducerMetricsRegistry;
import io.confluent.kafkarest.SystemTime;
import io.confluent.kafkarest.Time;
import io.confluent.kafkarest.controllers.ProduceController;
import io.confluent.kafkarest.controllers.RecordSerializer;
import io.confluent.kafkarest.controllers.SchemaManager;
import io.confluent.kafkarest.entities.EmbeddedFormat;
import io.confluent.kafkarest.entities.ProduceResult;
import io.confluent.kafkarest.entities.RegisteredSchema;
import io.confluent.kafkarest.entities.v3.ProduceRequest;
import io.confluent.kafkarest.entities.v3.ProduceRequest.ProduceRequestData;
import io.confluent.kafkarest.entities.v3.ProduceRequest.ProduceRequestHeader;
import io.confluent.kafkarest.entities.v3.ProduceResponse;
import io.confluent.kafkarest.entities.v3.ProduceResponse.ProduceResponseData;
import io.confluent.kafkarest.exceptions.BadRequestException;
import io.confluent.kafkarest.exceptions.RateLimitGracePeriodExceededException;
import io.confluent.kafkarest.extension.ResourceAccesslistFeature.ResourceName;
import io.confluent.kafkarest.resources.ProduceRateLimitCounters;
import io.confluent.kafkarest.response.ChunkedOutputFactory;
import io.confluent.kafkarest.response.StreamingResponseFactory;
import io.confluent.rest.annotations.PerformanceMetric;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collector;
import javax.inject.Inject;
import javax.inject.Provider;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.MediaType;

@Path("/v3/clusters/{clusterId}/topics/{topicName}/records")
@ResourceName("api.v3.produce.*")
public final class ProduceAction {

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
  private final Provider<ProducerMetrics> producerMetrics;
  private final ChunkedOutputFactory chunkedOutputFactory;
  private final StreamingResponseFactory streamingResponseFactory;

  private static final int ONE_SECOND = 1000;
  Time time = new SystemTime();
  private KafkaRestConfig config;

  @Inject
  public ProduceAction(
      Provider<SchemaManager> schemaManagerProvider,
      Provider<RecordSerializer> recordSerializer,
      Provider<ProduceController> produceControllerProvider,
      Provider<ProducerMetrics> producerMetrics,
      KafkaRestConfig config) {
    this.schemaManagerProvider = requireNonNull(schemaManagerProvider);
    this.recordSerializerProvider = requireNonNull(recordSerializer);
    this.produceControllerProvider = requireNonNull(produceControllerProvider);
    this.producerMetrics = requireNonNull(producerMetrics);
    this.config = config;
    this.chunkedOutputFactory = ChunkedOutputFactory.getChunkedOutputFactory();
    this.streamingResponseFactory = new StreamingResponseFactory(chunkedOutputFactory);
  }

  public ProduceAction(
      Provider<SchemaManager> schemaManagerProvider,
      Provider<RecordSerializer> recordSerializer,
      Provider<ProduceController> produceControllerProvider,
      Provider<ProducerMetrics> producerMetrics,
      KafkaRestConfig config,
      ChunkedOutputFactory chunkedOutputFactory,
      StreamingResponseFactory streamingResponseFactory) {
    this.schemaManagerProvider = requireNonNull(schemaManagerProvider);
    this.recordSerializerProvider = requireNonNull(recordSerializer);
    this.produceControllerProvider = requireNonNull(produceControllerProvider);
    this.producerMetrics = requireNonNull(producerMetrics);
    this.config = config;
    this.streamingResponseFactory = streamingResponseFactory;
    this.chunkedOutputFactory = chunkedOutputFactory;
  }

  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @PerformanceMetric("v3.produce.produce-to-topic")
  @ResourceName("api.v3.produce.produce-to-topic")
  public void produce(
      @Suspended AsyncResponse asyncResponse,
      @PathParam("clusterId") String clusterId,
      @PathParam("topicName") String topicName,
      MappingIterator<ProduceRequest> requests)
      throws Exception {

    if (rateLimit()) {
      long now = time.milliseconds();
      addToAndCullRateCounter(now);
      if (overRateLimit()) {
        if (overGracePeriod(now)) {
          streamingResponseFactory
              .from(requests)
              .compose(
                  request -> {
                    CompletableFuture future = new CompletableFuture();
                    future.completeExceptionally(
                        new RateLimitGracePeriodExceededException(
                            config.getInt(KafkaRestConfig.PRODUCE_MAX_REQUESTS_PER_SECOND),
                            config.getInt(KafkaRestConfig.PRODUCE_GRACE_PERIOD)));
                    return future;
                  })
              .resume(asyncResponse);
          return;
        }
      } else {
        ProduceRateLimitCounters.resetGracePeriodStart();
      }
    }
    AtomicBoolean firstMessage = new AtomicBoolean(true);

    ProduceController controller = produceControllerProvider.get();
    streamingResponseFactory
        .from(requests)
        .compose(
            request -> {
              if (rateLimit()) {
                if (!firstMessage.get()) {
                  long streamedNow = time.milliseconds();
                  addToAndCullRateCounter(streamedNow);
                  if (overRateLimit()) {
                    if (overGracePeriod(streamedNow)) {
                      CompletableFuture future = new CompletableFuture();
                      future.completeExceptionally(
                          new RateLimitGracePeriodExceededException(
                              config.getInt(KafkaRestConfig.PRODUCE_MAX_REQUESTS_PER_SECOND),
                              config.getInt(KafkaRestConfig.PRODUCE_GRACE_PERIOD)));
                      return future;
                    }
                  } else {
                    ProduceRateLimitCounters.resetGracePeriodStart();
                  }
                } else {
                  firstMessage.set(false);
                }
              }
              return produce(clusterId, topicName, request, controller);
            })
        .resume(asyncResponse);
  }

  private CompletableFuture<ProduceResponse> produce(
      String clusterId, String topicName, ProduceRequest request, ProduceController controller) {

    Instant requestInstant = Instant.now();
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

    recordRequestMetrics(request.getOriginalSize().orElse(0L));

    CompletableFuture<ProduceResult> produceResult =
        controller.produce(
            clusterId,
            topicName,
            request.getPartitionId(),
            request.getHeaders().stream().collect(PRODUCE_REQUEST_HEADER_COLLECTOR),
            serializedKey,
            serializedValue,
            request.getTimestamp().orElse(Instant.now()));

    final Optional<Long> resumeAfterMs;
    if (overRateLimit()) {
      resumeAfterMs =
          Optional.of(
              (long)
                      (ProduceRateLimitCounters.size()
                              / config.getInt(KafkaRestConfig.PRODUCE_MAX_REQUESTS_PER_SECOND)
                          - 1)
                  * 1000);
    } else {
      resumeAfterMs = Optional.empty();
    }

    return produceResult
        .handle(
            (result, ex) -> {
              if (ex != null) {
                long latency =
                    Duration.between(requestInstant, result.getCompletionTimestamp()).toMillis();
                recordErrorMetrics(latency);
                throw new CompletionException(ex);
              }
              return result;
            })
        .thenApply(
            result -> {
              ProduceResponse response =
                  toProduceResponse(
                      clusterId,
                      topicName,
                      keyFormat,
                      keySchema,
                      valueFormat,
                      valueSchema,
                      result,
                      resumeAfterMs);
              long latency =
                  Duration.between(requestInstant, result.getCompletionTimestamp()).toMillis();
              recordResponseMetrics(latency);
              return response;
            });
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
    } catch (IllegalArgumentException e) {
      throw new BadRequestException(e.getMessage(), e);
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

  private static ProduceResponse toProduceResponse(
      String clusterId,
      String topicName,
      Optional<EmbeddedFormat> keyFormat,
      Optional<RegisteredSchema> keySchema,
      Optional<EmbeddedFormat> valueFormat,
      Optional<RegisteredSchema> valueSchema,
      ProduceResult result,
      Optional<Long> resumeAfterMs) {
    return ProduceResponse.builder()
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
        .setResumeAfterMs(resumeAfterMs)
        .build();
  }

  private void recordResponseMetrics(long latency) {
    producerMetrics
        .get()
        .mbean(ProducerMetricsRegistry.GROUP_NAME, Collections.emptyMap())
        .recordResponse();
    producerMetrics
        .get()
        .mbean(ProducerMetricsRegistry.GROUP_NAME, Collections.emptyMap())
        .recordRequestLatency(latency);
  }

  private void recordErrorMetrics(long latency) {
    producerMetrics
        .get()
        .mbean(ProducerMetricsRegistry.GROUP_NAME, Collections.emptyMap())
        .recordError();
    producerMetrics
        .get()
        .mbean(ProducerMetricsRegistry.GROUP_NAME, Collections.emptyMap())
        .recordRequestLatency(latency);
  }

  private void recordRequestMetrics(long size) {
    producerMetrics
        .get()
        .mbean(ProducerMetricsRegistry.GROUP_NAME, Collections.emptyMap())
        .recordRequest();
    // record request size
    producerMetrics
        .get()
        .mbean(ProducerMetricsRegistry.GROUP_NAME, Collections.emptyMap())
        .recordRequestSize(size);
  }

  private void addToAndCullRateCounter(long now) {
    ProduceRateLimitCounters.add(now);
    while (ProduceRateLimitCounters.peek() < now - ONE_SECOND) {
      ProduceRateLimitCounters.poll();
    }
    if (ProduceRateLimitCounters.size()
        <= config.getInt(KafkaRestConfig.PRODUCE_MAX_REQUESTS_PER_SECOND)) {
      ProduceRateLimitCounters.resetGracePeriodStart();
    }
  }

  private boolean rateLimit() {
    return config.getBoolean(KafkaRestConfig.PRODUCE_RATE_LIMIT_ENABLED);
  }

  private boolean overRateLimit() {
    return ProduceRateLimitCounters.size()
        > config.getInt(KafkaRestConfig.PRODUCE_MAX_REQUESTS_PER_SECOND);
  }

  private boolean overGracePeriod(Long now) {
    if (!ProduceRateLimitCounters.gracePeriodPresent()
        && config.getInt(KafkaRestConfig.PRODUCE_GRACE_PERIOD) != 0) {
      ProduceRateLimitCounters.setGracePeriodStart(Optional.of(new AtomicLong(now)));
      return false;
    } else if (config.getInt(KafkaRestConfig.PRODUCE_GRACE_PERIOD) == 0
        || config.getInt(KafkaRestConfig.PRODUCE_GRACE_PERIOD)
            < now - ProduceRateLimitCounters.get()) {
      return true;
    }
    return false;
  }
}
