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

package io.confluent.kafkarest.resources.v2;

import static io.confluent.kafkarest.Errors.KAFKA_ERROR_ERROR_CODE;
import static java.util.Objects.requireNonNull;

import com.fasterxml.jackson.databind.node.NullNode;
import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableMultimap;
import com.google.protobuf.ByteString;
import io.confluent.kafkarest.Errors;
import io.confluent.kafkarest.common.CompletableFutures;
import io.confluent.kafkarest.controllers.ProduceController;
import io.confluent.kafkarest.controllers.RecordSerializer;
import io.confluent.kafkarest.controllers.SchemaManager;
import io.confluent.kafkarest.entities.EmbeddedFormat;
import io.confluent.kafkarest.entities.ProduceResult;
import io.confluent.kafkarest.entities.RegisteredSchema;
import io.confluent.kafkarest.entities.v2.PartitionOffset;
import io.confluent.kafkarest.entities.v2.ProduceRequest;
import io.confluent.kafkarest.entities.v2.ProduceRequest.ProduceRecord;
import io.confluent.kafkarest.entities.v2.ProduceResponse;
import io.confluent.rest.exceptions.RestServerErrorException;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.inject.Provider;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.RetriableException;

abstract class AbstractProduceAction {

  public static final String UNEXPECTED_PRODUCER_EXCEPTION =
      "Unexpected non-Kafka exception returned by Kafka";

  private final Provider<SchemaManager> schemaManager;
  private final Provider<RecordSerializer> recordSerializer;
  private final Provider<ProduceController> produceController;

  AbstractProduceAction(
      Provider<SchemaManager> schemaManager,
      Provider<RecordSerializer> recordSerializer,
      Provider<ProduceController> produceController) {
    this.schemaManager = requireNonNull(schemaManager);
    this.recordSerializer = requireNonNull(recordSerializer);
    this.produceController = requireNonNull(produceController);
  }

  final CompletableFuture<ProduceResponse> produceWithoutSchema(
      EmbeddedFormat format,
      String topicName,
      Optional<Integer> partition,
      ProduceRequest request) {
    List<SerializedKeyAndValue> serialized =
        serialize(
            format,
            topicName,
            partition,
            /* keySchema= */ Optional.empty(),
            /* valueSchema= */ Optional.empty(),
            request.getRecords());

    List<CompletableFuture<ProduceResult>> resultFutures = doProduce(topicName, serialized);

    return produceResultsToResponse(
        /* keySchema= */ Optional.empty(), /* valueSchema= */ Optional.empty(), resultFutures);
  }

  final CompletableFuture<ProduceResponse> produceWithSchema(
      EmbeddedFormat format,
      String topicName,
      Optional<Integer> partition,
      ProduceRequest request) {
    Optional<RegisteredSchema> keySchema =
        getSchema(
            format,
            topicName,
            request.getKeySchemaId(),
            request.getKeySchema(),
            /* isKey= */ true);
    Optional<RegisteredSchema> valueSchema =
        getSchema(
            format,
            topicName,
            request.getValueSchemaId(),
            request.getValueSchema(),
            /* isKey= */ false);

    List<SerializedKeyAndValue> serialized =
        serialize(
            format,
            topicName,
            partition,
            keySchema,
            valueSchema,
            request.getRecords());

    List<CompletableFuture<ProduceResult>> resultFutures = doProduce(topicName, serialized);

    return produceResultsToResponse(keySchema, valueSchema, resultFutures);
  }

  private Optional<RegisteredSchema> getSchema(
      EmbeddedFormat format,
      String topicName,
      Optional<Integer> schemaId,
      Optional<String> schema,
      boolean isKey) {
    if (format.requiresSchema() && (schemaId.isPresent() || schema.isPresent())) {
      return Optional.of(
          schemaManager.get().getSchema(
              /* topicName= */ topicName,
              /* format= */ schema.map(unused -> format),
              /* subject= */ Optional.empty(),
              /* subjectNameStrategy= */ Optional.empty(),
              /* schemaId= */ schemaId,
              /* schemaVersion= */ Optional.empty(),
              /* rawSchema= */ schema,
              /* isKey= */ isKey));
    } else {
      return Optional.empty();
    }
  }

  private List<SerializedKeyAndValue> serialize(
      EmbeddedFormat format,
      String topicName,
      Optional<Integer> partition,
      Optional<RegisteredSchema> keySchema,
      Optional<RegisteredSchema> valueSchema,
      List<ProduceRecord> records) {
    return records.stream()
        .map(
            record ->
                SerializedKeyAndValue.create(
                    record.getPartition().map(Optional::of).orElse(partition),
                    recordSerializer.get()
                        .serialize(
                            format,
                            topicName,
                            keySchema,
                            record.getKey().orElse(NullNode.getInstance()),
                            /* isKey= */ true),
                    recordSerializer.get()
                        .serialize(
                            format,
                            topicName,
                            valueSchema,
                            record.getValue().orElse(NullNode.getInstance()),
                            /* isKey= */ false)))
        .collect(Collectors.toList());
  }

  private List<CompletableFuture<ProduceResult>> doProduce(
      String topicName, List<SerializedKeyAndValue> serialized) {
    return serialized.stream()
        .map(
            record ->
                produceController.get()
                    .produce(
                        /* clusterId= */ "",
                        topicName,
                        record.getPartitionId(),
                        /* headers= */ ImmutableMultimap.of(),
                        record.getKey(),
                        record.getValue(),
                        /* timestamp= */ Instant.now()))
        .collect(Collectors.toList());
  }

  private static CompletableFuture<ProduceResponse> produceResultsToResponse(
      Optional<RegisteredSchema> keySchema,
      Optional<RegisteredSchema> valueSchema,
      List<CompletableFuture<ProduceResult>> resultFutures
  ) {
    CompletableFuture<List<PartitionOffset>> offsetsFuture =
        CompletableFutures.allAsList(
            resultFutures.stream()
                .map(
                    future ->
                        future.thenApply(
                            result ->
                                new PartitionOffset(
                                    result.getPartitionId(),
                                    result.getOffset(),
                                    /* errorCode= */ null,
                                    /* error= */ null)))
                .map(
                    future ->
                        future.exceptionally(
                            throwable ->
                                new PartitionOffset(
                                    /* partition= */ null,
                                    /* offset= */ null,
                                    errorCodeFromProducerException(throwable.getCause()),
                                    throwable.getCause().getMessage())))
                .collect(Collectors.toList()));

    return offsetsFuture.thenApply(
        offsets ->
            new ProduceResponse(
                offsets,
                keySchema.map(RegisteredSchema::getSchemaId).orElse(null),
                valueSchema.map(RegisteredSchema::getSchemaId).orElse(null)));
  }

  private static int errorCodeFromProducerException(Throwable e) {
    if (e instanceof AuthenticationException) {
      return Errors.KAFKA_AUTHENTICATION_ERROR_CODE;
    } else if (e instanceof AuthorizationException) {
      return Errors.KAFKA_AUTHORIZATION_ERROR_CODE;
    } else if (e instanceof RetriableException) {
      return Errors.KAFKA_RETRIABLE_ERROR_ERROR_CODE;
    } else if (e instanceof KafkaException) {
      return KAFKA_ERROR_ERROR_CODE;
    } else {
      // We shouldn't see any non-Kafka exceptions, but this covers us in case we do see an
      // unexpected error. In that case we fail the entire request -- this loses information
      // since some messages may have been produced correctly, but is the right thing to do from
      // a REST perspective since there was an internal error with the service while processing
      // the request.
      throw new RestServerErrorException(UNEXPECTED_PRODUCER_EXCEPTION,
          RestServerErrorException.DEFAULT_ERROR_CODE, e
      );
    }
  }

  @AutoValue
  abstract static class SerializedKeyAndValue {

    abstract Optional<Integer> getPartitionId();

    abstract Optional<ByteString> getKey();

    abstract Optional<ByteString> getValue();

    private static SerializedKeyAndValue create(
        Optional<Integer> partitionId, Optional<ByteString> key, Optional<ByteString> value) {
      return new AutoValue_AbstractProduceAction_SerializedKeyAndValue(partitionId, key, value);
    }
  }
}
