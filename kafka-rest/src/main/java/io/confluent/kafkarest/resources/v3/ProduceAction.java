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
import io.confluent.kafka.serializers.subject.strategy.SubjectNameStrategy;
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
import io.confluent.kafkarest.exceptions.StatusCodeException;
import io.confluent.kafkarest.response.StreamingResponse;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
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
import javax.ws.rs.core.Response.Status;

@Path("/v3/clusters/{clusterId}/topics/{topicName}/records")
public final class ProduceAction {

  private static final Collector<
      ProduceRequestHeader,
      ImmutableMultimap.Builder<String, Optional<ByteString>>,
      ImmutableMultimap<String, Optional<ByteString>>> PRODUCE_REQUEST_HEADER_COLLECTOR =
      Collector.of(
          ImmutableMultimap::builder,
          (builder, header) -> builder.put(header.getName(), header.getValue()),
          (left, right) -> left.putAll(right.build()),
          ImmutableMultimap.Builder::build);

  private final Provider<SchemaManager> schemaManager;
  private final Provider<RecordSerializer> recordSerializer;
  private final Provider<ProduceController> produceController;
  private final SubjectNameStrategy defaultSubjectNameStrategy;

  @Inject
  public ProduceAction(
      Provider<SchemaManager> schemaManager,
      Provider<RecordSerializer> recordSerializer,
      Provider<ProduceController> produceController,
      SubjectNameStrategy defaultSubjectNameStrategy) {
    this.schemaManager = requireNonNull(schemaManager);
    this.recordSerializer = requireNonNull(recordSerializer);
    this.produceController = requireNonNull(produceController);
    this.defaultSubjectNameStrategy = requireNonNull(defaultSubjectNameStrategy);
  }

  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public void produce(
      @Suspended AsyncResponse asyncResponse,
      @PathParam("clusterId") String clusterId,
      @PathParam("topicName") String topicName,
      MappingIterator<ProduceRequest> requests) throws Exception {
    StreamingResponse.from(requests)
        .compose(request -> produce(clusterId, topicName, request))
        .resume(asyncResponse);
  }

  private CompletableFuture<ProduceResponse> produce(
      String clusterId, String topicName, ProduceRequest request) {
    Optional<RegisteredSchema> keySchema =
        request.getKey().flatMap(key -> getSchema(topicName, /* isKey= */ true, key));
    Optional<EmbeddedFormat> keyFormat =
        keySchema.map(schema -> Optional.of(schema.getFormat()))
            .orElse(request.getKey().flatMap(ProduceRequestData::getFormat));
    Optional<ByteString> serializedKey =
        serialize(topicName, keyFormat, keySchema, request.getKey(), /* isKey= */ true);

    Optional<RegisteredSchema> valueSchema =
        request.getValue().flatMap(value -> getSchema(topicName, /* isKey= */ false, value));
    Optional<EmbeddedFormat> valueFormat =
        valueSchema.map(schema -> Optional.of(schema.getFormat()))
            .orElse(request.getValue().flatMap(ProduceRequestData::getFormat));
    Optional<ByteString> serializedValue =
        serialize(topicName, valueFormat, valueSchema, request.getValue(), /* isKey= */ false);

    CompletableFuture<ProduceResult> produceResult =
        produceController.get().produce(
            clusterId,
            topicName,
            request.getPartitionId(),
            request.getHeaders().stream().collect(PRODUCE_REQUEST_HEADER_COLLECTOR),
            serializedKey,
            serializedValue,
            request.getTimestamp().orElse(Instant.now()));

    return produceResult.thenApply(
        result ->
            toProduceResponse(
                clusterId, topicName, keyFormat, keySchema, valueFormat, valueSchema, result));
  }

  private Optional<RegisteredSchema> getSchema(
      String topicName, boolean isKey, ProduceRequestData data) {
    // format
    if (data.getFormat().isPresent() && !data.getRawSchema().isPresent()) {
      // If format != null and schema = null, then this is a schemaless format.
      return Optional.empty();
    }

    // format, (schema_subject|schema_subject_strategy)?, schema
    if (data.getFormat().isPresent() && data.getRawSchema().isPresent()) {
      return Optional.of(
          getSchemaFromRawSchema(
              topicName,
              isKey,
              data,
              data.getFormat().get(),
              data.getRawSchema().get()));
    }

    // (schema_subject|schema_subject_strategy)?, schema_id
    if (data.getSchemaId().isPresent()) {
      return Optional.of(
          getSchemaFromSchemaId(
              topicName,
              isKey,
              data,
              data.getSchemaId().get()));
    }

    // (schema_subject|schema_subject_strategy)?, schema_version
    if (data.getSchemaVersion().isPresent()) {
      return Optional.of(
          getSchemaFromSchemaVersion(
              topicName,
              isKey,
              data,
              data.getSchemaVersion().get()));
    }

    // (schema_subject|schema_subject_strategy)?
    return Optional.of(getLatestSchema(topicName, isKey, data));
  }

  private RegisteredSchema getSchemaFromRawSchema(
      String topicName,
      boolean isKey,
      ProduceRequestData data,
      EmbeddedFormat format,
      String rawSchema) {
    // format, schema_subject, schema
    if (data.getSchemaSubject().isPresent()) {
      return schemaManager.get().parseSchema(
          format,
          data.getSchemaSubject().get(),
          rawSchema);
    }

    // format, schema_subject_strategy, schema
    if (data.getSchemaSubjectStrategy().isPresent()) {
      return schemaManager.get().parseSchema(
          format,
          data.getSchemaSubjectStrategy().get(),
          topicName,
          isKey,
          rawSchema);
    }

    // format, schema
    return schemaManager.get().parseSchema(
        format,
        defaultSubjectNameStrategy,
        topicName,
        isKey,
        rawSchema);
  }

  private RegisteredSchema getSchemaFromSchemaId(
      String topicName,
      boolean isKey,
      ProduceRequestData data,
      int schemaId) {
    // schema_subject, schema_id
    if (data.getSchemaSubject().isPresent()) {
      return schemaManager.get().getSchemaById(
          data.getSchemaSubject().get(),
          schemaId);
    }

    // schema_subject_strategy, schema_id
    if (data.getSchemaSubjectStrategy().isPresent()) {
      return schemaManager.get().getSchemaById(
          data.getSchemaSubjectStrategy().get(),
          topicName,
          isKey,
          schemaId);
    }

    // schema_id
    return schemaManager.get().getSchemaById(
        defaultSubjectNameStrategy,
        topicName,
        isKey,
        schemaId);
  }

  private RegisteredSchema getSchemaFromSchemaVersion(
      String topicName,
      boolean isKey,
      ProduceRequestData data,
      int schemaVersion) {
    // schema_subject, schema_version
    if (data.getSchemaSubject().isPresent()) {
      return schemaManager.get().getSchemaByVersion(
          data.getSchemaSubject().get(),
          schemaVersion);
    }

    // schema_subject_strategy?, schema_version
    return schemaManager.get().getSchemaByVersion(
        getSchemaSubjectUnsafe(topicName, isKey, data),
        schemaVersion);
  }

  private RegisteredSchema getLatestSchema(
      String topicName,
      boolean isKey,
      ProduceRequestData data) {
    // schema_subject
    if (data.getSchemaSubject().isPresent()) {
      return schemaManager.get().getLatestSchema(data.getSchemaSubject().get());
    }

    // schema_subject_strategy?
    return schemaManager.get().getLatestSchema(getSchemaSubjectUnsafe(topicName, isKey, data));
  }

  /**
   * Tries to get the schema subject from only schema_subject_strategy, {@code topicName} and {@code
   * isKey}.
   *
   * <p>This operation  is only really supported if schema_subject_strategy does not depend on the
   * parsed schema to generate the subject name, as we need the subject name to fetch the schema
   * by version. That's the case, for example, of TopicNameStrategy
   * (schema_subject_strategy=TOPIC_NAME). Since TopicNameStrategy is so popular, instead of
   * requiring users to always specify schema_subject if using schema_version?, we try using the
   * strategy to generate the subject name, and fail if that does not work out.
   */
  private String getSchemaSubjectUnsafe(String topicName, boolean isKey, ProduceRequestData data) {
    SubjectNameStrategy subjectNameStrategy;
    if (data.getSchemaSubjectStrategy().isPresent()) {
      subjectNameStrategy = data.getSchemaSubjectStrategy().get();
    } else {
      subjectNameStrategy = defaultSubjectNameStrategy;
    }

    String subject = null;
    Exception cause = null;
    try {
      subject = subjectNameStrategy.subjectName(topicName, isKey, /* schema= */ null);
    } catch (Exception e) {
      cause = e;
    }

    if (subject == null) {
      StatusCodeException error =
          StatusCodeException.create(
              Status.BAD_REQUEST,
              "Bad Request",
              String.format(
                  "Cannot use%s schema_subject_strategy%s without schema_id or schema.",
                  data.getSchemaSubjectStrategy().map(strategy -> "").orElse(" default"),
                  data.getSchemaSubjectStrategy().map(strategy -> "=" + strategy).orElse("")));
      if (cause != null) {
        error.initCause(cause);
      }
      throw error;
    }

    return subject;
  }

  private Optional<ByteString> serialize(
      String topicName,
      Optional<EmbeddedFormat> format,
      Optional<RegisteredSchema> schema,
      Optional<ProduceRequestData> data,
      boolean isKey) {
    return recordSerializer.get()
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
                        .setSchemaSubject(keySchema.map(RegisteredSchema::getSubject))
                        .setSchemaId(keySchema.map(RegisteredSchema::getSchemaId))
                        .setSchemaVersion(keySchema.map(RegisteredSchema::getSchemaVersion))
                        .setSize(result.getSerializedKeySize())
                        .build()))
        .setValue(
            valueFormat.map(
                format ->
                    ProduceResponseData.builder()
                        .setType(valueFormat)
                        .setSchemaSubject(valueSchema.map(RegisteredSchema::getSubject))
                        .setSchemaId(valueSchema.map(RegisteredSchema::getSchemaId))
                        .setSchemaVersion(valueSchema.map(RegisteredSchema::getSchemaVersion))
                        .setSize(result.getSerializedValueSize())
                        .build()))
        .build();
  }
}
