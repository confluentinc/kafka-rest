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

package io.confluent.kafkarest.entities.v3;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.BinaryNode;
import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.serializers.subject.RecordNameStrategy;
import io.confluent.kafka.serializers.subject.TopicNameStrategy;
import io.confluent.kafka.serializers.subject.TopicRecordNameStrategy;
import io.confluent.kafka.serializers.subject.strategy.SubjectNameStrategy;
import io.confluent.kafkarest.entities.EmbeddedFormat;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nullable;

@AutoValue
public abstract class ProduceRequest {

  ProduceRequest() {
  }

  @JsonProperty("partition_id")
  @JsonInclude(Include.NON_ABSENT)
  public abstract Optional<Integer> getPartitionId();

  @JsonProperty("headers")
  @JsonInclude(Include.NON_EMPTY)
  public abstract ImmutableList<ProduceRequestHeader> getHeaders();

  @JsonProperty("key")
  @JsonInclude(Include.NON_ABSENT)
  public abstract Optional<ProduceRequestData> getKey();

  @JsonProperty("value")
  @JsonInclude(Include.NON_ABSENT)
  public abstract Optional<ProduceRequestData> getValue();

  @JsonProperty("timestamp")
  @JsonInclude(Include.NON_ABSENT)
  public abstract Optional<Instant> getTimestamp();

  @JsonCreator
  static ProduceRequest fromJson(
      @JsonProperty("partition_id") @Nullable Integer partitionId,
      @JsonProperty("headers") @Nullable List<ProduceRequestHeader> headers,
      @JsonProperty("key") @Nullable ProduceRequestData key,
      @JsonProperty("value") @Nullable ProduceRequestData value,
      @JsonProperty("timestamp") @Nullable Instant timestamp) {
    return builder()
        .setPartitionId(partitionId)
        .setHeaders(headers != null ? headers : ImmutableList.of())
        .setKey(key)
        .setValue(value)
        .setTimestamp(timestamp)
        .build();
  }

  public static Builder builder() {
    return new AutoValue_ProduceRequest.Builder().setHeaders(emptyList());
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setPartitionId(@Nullable Integer partitionId);

    public abstract Builder setHeaders(List<ProduceRequestHeader> headers);

    public abstract Builder setKey(@Nullable ProduceRequestData key);

    public abstract Builder setValue(@Nullable ProduceRequestData value);

    public abstract Builder setTimestamp(@Nullable Instant timestamp);

    public abstract ProduceRequest build();
  }

  @AutoValue
  public abstract static class ProduceRequestHeader {

    ProduceRequestHeader() {
    }

    @JsonProperty("name")
    public abstract String getName();

    @JsonIgnore
    public abstract Optional<ByteString> getValue();

    @JsonProperty("value")
    @JsonInclude(Include.NON_ABSENT)
    final Optional<BinaryNode> getSerializedValue() {
      return getValue().map(value -> BinaryNode.valueOf(value.toByteArray()));
    }

    public static ProduceRequestHeader create(String name, @Nullable ByteString value) {
      return new AutoValue_ProduceRequest_ProduceRequestHeader(name, Optional.ofNullable(value));
    }

    @JsonCreator
    static ProduceRequestHeader fromJson(
        @JsonProperty("name") String name, @JsonProperty("value") @Nullable byte[] value) {
      return create(name, value != null ? ByteString.copyFrom(value) : null);
    }
  }

  @AutoValue
  public abstract static class ProduceRequestData {

    ProduceRequestData() {
    }

    @JsonProperty("type")
    @JsonInclude(Include.NON_ABSENT)
    public abstract Optional<EmbeddedFormat> getFormat();

    @JsonProperty("schema_subject")
    @JsonInclude(Include.NON_ABSENT)
    public abstract Optional<String> getSchemaSubject();

    @JsonProperty("schema_subject_strategy")
    @JsonInclude(Include.NON_ABSENT)
    public abstract Optional<SchemaSubjectStrategy> getSchemaSubjectStrategy();

    @JsonProperty("schema_id")
    @JsonInclude(Include.NON_ABSENT)
    public abstract Optional<Integer> getSchemaId();

    @JsonProperty("schema_version")
    @JsonInclude(Include.NON_ABSENT)
    public abstract Optional<Integer> getSchemaVersion();

    @JsonProperty("schema")
    @JsonInclude(Include.NON_ABSENT)
    public abstract Optional<String> getRawSchema();

    @JsonProperty("data")
    public abstract JsonNode getData();

    public static Builder builder() {
      return new AutoValue_ProduceRequest_ProduceRequestData.Builder();
    }

    @JsonCreator
    static ProduceRequestData fromJson(
        @JsonProperty("schema_subject_strategy") @Nullable
            SchemaSubjectStrategy schemaSubjectStrategy,
        @JsonProperty("type") @Nullable EmbeddedFormat format,
        @JsonProperty("schema_subject") @Nullable String schemaSubject,
        @JsonProperty("schema_id") @Nullable Integer schemaId,
        @JsonProperty("schema_version") @Nullable Integer schemaVersion,
        @JsonProperty("schema") @Nullable String rawSchema,
        @JsonProperty("data") JsonNode data) {
      return builder()
          .setFormat(format)
          .setSchemaSubjectStrategy(schemaSubjectStrategy)
          .setSchemaSubject(schemaSubject)
          .setSchemaId(schemaId)
          .setSchemaVersion(schemaVersion)
          .setRawSchema(rawSchema)
          .setData(data)
          .build();
    }

    @AutoValue.Builder
    public abstract static class Builder {

      Builder() {
      }

      public abstract Builder setFormat(@Nullable EmbeddedFormat format);

      public abstract Builder setSchemaSubjectStrategy(
          @Nullable SchemaSubjectStrategy schemaSubjectStrategy);

      public abstract Builder setSchemaSubject(@Nullable String schemaSubject);

      public abstract Builder setSchemaId(@Nullable Integer getSchemaId);

      public abstract Builder setSchemaVersion(@Nullable Integer schemaVersion);

      public abstract Builder setRawSchema(@Nullable String rawSchema);

      public abstract Builder setData(JsonNode data);

      abstract ProduceRequestData autoBuild();

      public final ProduceRequestData build() {
        ProduceRequestData request = autoBuild();

        checkState(
            !request.getSchemaSubjectStrategy().isPresent()
                || !request.getSchemaSubject().isPresent(),
            "Only one of 'schema_subject_strategy' or 'schema_subject' can be used.");

        checkState(
            request.getSchemaId().isPresent()
                ? (!request.getSchemaVersion().isPresent() && !request.getRawSchema().isPresent())
                : (!request.getSchemaVersion().isPresent() || !request.getRawSchema().isPresent()),
            "Only one of 'schema_id', 'schema_version' or 'schema' can be used.");

        if (request.getFormat().isPresent()) {
          if (request.getFormat().get().requiresSchema()) {
            checkState(
                !request.getSchemaId().isPresent(),
                "'schema_id=%s' cannot be used with 'serializer'.",
                request.getSchemaId().orElse(null));
            checkState(
                !request.getSchemaVersion().isPresent(),
                "'schema_version=%s' cannot be used with 'serializer'.",
                request.getSchemaVersion().orElse(null));
            checkState(
                request.getRawSchema().isPresent(),
                "'schema_version=latest' cannot be used with 'serializer'.");
          } else {
            checkState(
                !request.getSchemaSubjectStrategy().isPresent(),
                "'serializer=%s' cannot be used with 'schema_subject_strategy'.",
                request.getFormat().orElse(null));
            checkState(
                !request.getSchemaSubject().isPresent(),
                "'serializer=%s' cannot be used with 'schema_subject'.",
                request.getFormat().orElse(null));
            checkState(
                !request.getSchemaId().isPresent(),
                "'serializer=%s' cannot be used with 'schema_id'.",
                request.getFormat().orElse(null));
            checkState(
                !request.getSchemaVersion().isPresent(),
                "'serializer=%s' cannot be used with 'schema_version'.",
                request.getFormat().orElse(null));
            checkState(
                !request.getRawSchema().isPresent(),
                "'serializer=%s' cannot be used with 'schema'.",
                request.getFormat().orElse(null));
          }
        }

        return request;
      }
    }
  }

  public enum SchemaSubjectStrategy implements SubjectNameStrategy {

    /**
     * See {@link TopicNameStrategy}.
     */
    TOPIC_NAME(new TopicNameStrategy()),

    /**
     * See {@link RecordNameStrategy}.
     */
    RECORD_NAME(new RecordNameStrategy()),

    /**
     * See {@link TopicRecordNameStrategy}.
     */
    TOPIC_RECORD_NAME(new TopicRecordNameStrategy());

    private final SubjectNameStrategy delegate;

    SchemaSubjectStrategy(SubjectNameStrategy delegate) {
      this.delegate = requireNonNull(delegate);
    }

    @Override
    public String subjectName(String topic, boolean isKey, ParsedSchema schema) {
      return delegate.subjectName(topic, isKey, schema);
    }

    @Override
    public void configure(Map<String, ?> configs) {
      delegate.configure(configs);
    }
  }
}
