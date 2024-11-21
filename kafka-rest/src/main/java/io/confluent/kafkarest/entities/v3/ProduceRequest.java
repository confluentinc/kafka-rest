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
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
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
import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nullable;

@AutoValue
@JsonDeserialize(using = ProduceRequest.Deserializer.class)
public abstract class ProduceRequest {

  ProduceRequest() {}

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

  @JsonIgnore
  public abstract long getOriginalSize();

  public static Builder builder() {
    return new AutoValue_ProduceRequest.Builder().setHeaders(emptyList());
  }

  @AutoValue.Builder
  public abstract static class Builder {

    @JsonCreator
    static Builder fromJson(
        @JsonProperty("partition_id") @Nullable Integer partitionId,
        @JsonProperty("headers") @Nullable List<ProduceRequestHeader> headers,
        @JsonProperty("key") @Nullable ProduceRequestData key,
        @JsonProperty("value") @Nullable ProduceRequestData value,
        @JsonProperty("timestamp") @Nullable Instant timestamp) {
      return ProduceRequest.builder()
          .setPartitionId(partitionId)
          .setHeaders(headers != null ? headers : ImmutableList.of())
          .setKey(key)
          .setValue(value)
          .setTimestamp(timestamp);
    }

    public abstract Builder setPartitionId(@Nullable Integer partitionId);

    public abstract Builder setHeaders(List<ProduceRequestHeader> headers);

    public abstract Builder setKey(@Nullable ProduceRequestData key);

    public abstract Builder setValue(@Nullable ProduceRequestData value);

    public abstract Builder setTimestamp(@Nullable Instant timestamp);

    public abstract Builder setOriginalSize(long size);

    public abstract ProduceRequest build();
  }

  static final class Deserializer extends JsonDeserializer<ProduceRequest> {

    @Override
    public ProduceRequest deserialize(JsonParser parser, DeserializationContext context)
        throws IOException {
      long start =
          parser.getCurrentLocation().getByteOffset() == -1
              ? parser.getCurrentLocation().getCharOffset()
              : parser.getCurrentLocation().getByteOffset();
      ProduceRequest.Builder builder = parser.readValueAs(ProduceRequest.Builder.class);
      long end =
          parser.getCurrentLocation().getByteOffset() == -1
              ? parser.getCurrentLocation().getCharOffset()
              : parser.getCurrentLocation().getByteOffset();
      long size = start == -1 || end == -1 ? 0 : end - start + 1;
      builder.setOriginalSize(size);
      return builder.build();
    }
  }

  @AutoValue
  public abstract static class ProduceRequestHeader {

    ProduceRequestHeader() {}

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

    ProduceRequestData() {}

    @JsonProperty("type")
    @JsonInclude(Include.NON_ABSENT)
    public abstract Optional<EmbeddedFormat> getFormat();

    @JsonProperty("subject")
    @JsonInclude(Include.NON_ABSENT)
    public abstract Optional<String> getSubject();

    @JsonProperty("subject_name_strategy")
    @JsonInclude(Include.NON_ABSENT)
    public abstract Optional<EnumSubjectNameStrategy> getSubjectNameStrategy();

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
        @JsonProperty("type") @Nullable EmbeddedFormat format,
        @JsonProperty("subject") @Nullable String subject,
        @JsonProperty("subject_name_strategy") @Nullable
            EnumSubjectNameStrategy subjectNameStrategy,
        @JsonProperty("schema_id") @Nullable Integer schemaId,
        @JsonProperty("schema_version") @Nullable Integer schemaVersion,
        @JsonProperty("schema") @Nullable String rawSchema,
        @JsonProperty("data") JsonNode data) {
      return builder()
          .setFormat(format)
          .setSubjectNameStrategy(subjectNameStrategy)
          .setSubject(subject)
          .setSchemaId(schemaId)
          .setSchemaVersion(schemaVersion)
          .setRawSchema(rawSchema)
          .setData(data)
          .build();
    }

    @AutoValue.Builder
    public abstract static class Builder {

      Builder() {}

      public abstract Builder setFormat(@Nullable EmbeddedFormat format);

      public abstract Builder setSubject(@Nullable String schemaSubject);

      public abstract Builder setSubjectNameStrategy(
          @Nullable EnumSubjectNameStrategy subjectNameStrategy);

      public abstract Builder setSchemaId(@Nullable Integer getSchemaId);

      public abstract Builder setSchemaVersion(@Nullable Integer schemaVersion);

      public abstract Builder setRawSchema(@Nullable String rawSchema);

      public abstract Builder setData(JsonNode data);

      abstract ProduceRequestData autoBuild();

      public final ProduceRequestData build() {
        ProduceRequestData request = autoBuild();

        checkState(
            !request.getSubjectNameStrategy().isPresent() || !request.getSubject().isPresent(),
            "Only one of 'subject_name_strategy' or 'subject' can be used.");

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
                !request.getSubjectNameStrategy().isPresent(),
                "'type=%s' cannot be used with 'subject_strategy'.",
                request.getFormat().get());
            checkState(
                !request.getSubject().isPresent(),
                "'type=%s' cannot be used with 'subject'.",
                request.getFormat().get());
            checkState(
                !request.getSchemaId().isPresent(),
                "'type=%s' cannot be used with 'schema_id'.",
                request.getFormat().get());
            checkState(
                !request.getSchemaVersion().isPresent(),
                "'type=%s' cannot be used with 'schema_version'.",
                request.getFormat().get());
            checkState(
                !request.getRawSchema().isPresent(),
                "'type=%s' cannot be used with 'schema'.",
                request.getFormat().get());
          }
        }

        return request;
      }
    }
  }

  public enum EnumSubjectNameStrategy implements SubjectNameStrategy {

    /** See {@link TopicNameStrategy}. */
    TOPIC_NAME(new TopicNameStrategy()),

    /** See {@link RecordNameStrategy}. */
    RECORD_NAME(new RecordNameStrategy()),

    /** See {@link TopicRecordNameStrategy}. */
    TOPIC_RECORD_NAME(new TopicRecordNameStrategy());

    private final SubjectNameStrategy delegate;

    EnumSubjectNameStrategy(SubjectNameStrategy delegate) {
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
