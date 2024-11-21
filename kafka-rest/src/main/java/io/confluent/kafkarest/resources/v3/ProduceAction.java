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
import io.confluent.kafkarest.Errors;
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
import io.confluent.kafkarest.exceptions.StacklessCompletionException;
import io.confluent.kafkarest.extension.ResourceAccesslistFeature.ResourceName;
import io.confluent.kafkarest.ratelimit.DoNotRateLimit;
import io.confluent.kafkarest.ratelimit.RateLimitExceededException;
import io.confluent.kafkarest.resources.v3.V3ResourcesModule.ProduceResponseThreadPool;
import io.confluent.kafkarest.response.StreamingResponseFactory;
import io.confluent.rest.annotations.PerformanceMetric;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
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
import org.apache.kafka.common.errors.SerializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@DoNotRateLimit
@Path("/v3/clusters/{clusterId}/topics/{topicName}/records")
@ResourceName("api.v3.produce.*")
public final class ProduceAction {

  private static final Logger log = LoggerFactory.getLogger(ProduceAction.class);

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
  private final StreamingResponseFactory streamingResponseFactory;
  private final ProduceRateLimiters produceRateLimiters;
  private final ExecutorService executorService;

  @Inject
  public ProduceAction(
      Provider<SchemaManager> schemaManagerProvider,
      Provider<RecordSerializer> recordSerializer,
      Provider<ProduceController> produceControllerProvider,
      Provider<ProducerMetrics> producerMetrics,
      StreamingResponseFactory streamingResponseFactory,
      ProduceRateLimiters produceRateLimiters,
      @ProduceResponseThreadPool ExecutorService executorService) {
    this.schemaManagerProvider = requireNonNull(schemaManagerProvider);
    this.recordSerializerProvider = requireNonNull(recordSerializer);
    this.produceControllerProvider = requireNonNull(produceControllerProvider);
    this.producerMetrics = requireNonNull(producerMetrics);
    this.streamingResponseFactory = requireNonNull(streamingResponseFactory);
    this.produceRateLimiters = requireNonNull(produceRateLimiters);
    this.executorService = requireNonNull(executorService);
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

    if (requests == null) {
      throw Errors.invalidPayloadException("Null input provided. Data is required.");
    }

    ProduceController controller = produceControllerProvider.get();
    streamingResponseFactory
        .from(requests)
        .compose(request -> produce(clusterId, topicName, request, controller))
        .resume(asyncResponse);
  }

  private CompletableFuture<ProduceResponse> produce(
      String clusterId, String topicName, ProduceRequest request, ProduceController controller) {

    try {
      produceRateLimiters.rateLimit(clusterId, request.getOriginalSize());
    } catch (RateLimitExceededException e) {
      // KREST-4356 Use our own CompletionException that will avoid the costly stack trace fill.
      throw new StacklessCompletionException(e);
    }

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

    recordRequestMetrics(request.getOriginalSize());

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
                long latency = Duration.between(requestInstant, Instant.now()).toMillis();
                recordErrorMetrics(latency);
                throw new StacklessCompletionException(error);
              }
              return result;
            },
            executorService)
        .thenApplyAsync(
            result -> {
              ProduceResponse response =
                  toProduceResponse(
                      clusterId, topicName, keyFormat, keySchema, valueFormat, valueSchema, result);
              long latency =
                  Duration.between(requestInstant, result.getCompletionTimestamp()).toMillis();
              recordResponseMetrics(latency);
              return response;
            },
            executorService);
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

  private static ProduceResponse toProduceResponse(
      String clusterId,
      String topicName,
      Optional<EmbeddedFormat> keyFormat,
      Optional<RegisteredSchema> keySchema,
      Optional<EmbeddedFormat> valueFormat,
      Optional<RegisteredSchema> valueSchema,
      ProduceResult result) {
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
        .build();
  }

  private void recordResponseMetrics(long latency) {
    producerMetrics.get().recordResponse();
    producerMetrics.get().recordRequestLatency(latency);
  }

  private void recordErrorMetrics(long latency) {
    producerMetrics.get().recordError();
    producerMetrics.get().recordRequestLatency(latency);
  }

  private void recordRequestMetrics(long size) {
    producerMetrics.get().recordRequest();
    // record request size
    producerMetrics.get().recordRequestSize(size);
  }
}
