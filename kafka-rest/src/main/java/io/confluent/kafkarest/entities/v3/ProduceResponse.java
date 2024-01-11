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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.auto.value.AutoValue;
import io.confluent.kafkarest.entities.EmbeddedFormat;
import java.time.Instant;
import java.util.Optional;
import javax.annotation.Nullable;

@AutoValue
public abstract class ProduceResponse {

  ProduceResponse() {}

  @JsonProperty("cluster_id")
  public abstract String getClusterId();

  @JsonProperty("topic_name")
  public abstract String getTopicName();

  @JsonProperty("partition_id")
  public abstract int getPartitionId();

  @JsonProperty("offset")
  public abstract long getOffset();

  @JsonProperty("timestamp")
  @JsonInclude(Include.NON_ABSENT)
  public abstract Optional<Instant> getTimestamp();

  @JsonProperty("key")
  @JsonInclude(Include.NON_ABSENT)
  public abstract Optional<ProduceResponseData> getKey();

  @JsonProperty("value")
  @JsonInclude(Include.NON_ABSENT)
  public abstract Optional<ProduceResponseData> getValue();

  @JsonCreator
  static ProduceResponse fromJson(
      @JsonProperty("cluster_id") String clusterId,
      @JsonProperty("topic_name") String topicName,
      @JsonProperty("partition_id") int partitionId,
      @JsonProperty("offset") long offset,
      @JsonProperty("timestamp") @Nullable Instant timestamp,
      @JsonProperty("key") @Nullable ProduceResponseData key,
      @JsonProperty("value") @Nullable ProduceResponseData value) {
    return builder()
        .setClusterId(clusterId)
        .setTopicName(topicName)
        .setPartitionId(partitionId)
        .setOffset(offset)
        .setTimestamp(timestamp)
        .setKey(key)
        .setValue(value)
        .build();
  }

  public static Builder builder() {
    return new AutoValue_ProduceResponse.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {

    Builder() {}

    public abstract Builder setClusterId(String clusterId);

    public abstract Builder setTopicName(String topicName);

    public abstract Builder setPartitionId(int partitionId);

    public abstract Builder setOffset(long offset);

    public abstract Builder setTimestamp(Optional<Instant> timestamp);

    public abstract Builder setTimestamp(@Nullable Instant timestamp);

    public abstract Builder setKey(Optional<ProduceResponseData> key);

    public abstract Builder setKey(@Nullable ProduceResponseData key);

    public abstract Builder setValue(Optional<ProduceResponseData> value);

    public abstract Builder setValue(@Nullable ProduceResponseData value);

    public abstract ProduceResponse build();
  }

  @AutoValue
  public abstract static class ProduceResponseData {

    ProduceResponseData() {}

    @JsonProperty("type")
    @JsonInclude(Include.NON_ABSENT)
    public abstract Optional<EmbeddedFormat> getType();

    @JsonProperty("subject")
    @JsonInclude(Include.NON_ABSENT)
    public abstract Optional<String> getSubject();

    @JsonProperty("schema_id")
    @JsonInclude(Include.NON_ABSENT)
    public abstract Optional<Integer> getSchemaId();

    @JsonProperty("schema_version")
    @JsonInclude(Include.NON_ABSENT)
    public abstract Optional<Integer> getSchemaVersion();

    @JsonProperty("size")
    public abstract int getSize();

    @JsonCreator
    static ProduceResponseData fromJson(
        @JsonProperty("type") @Nullable EmbeddedFormat type,
        @JsonProperty("subject") @Nullable String subject,
        @JsonProperty("schema_id") @Nullable Integer schemaId,
        @JsonProperty("schema_version") @Nullable Integer schemaVersion,
        @JsonProperty("size") int size) {
      return builder()
          .setType(type)
          .setSubject(subject)
          .setSchemaId(schemaId)
          .setSchemaVersion(schemaVersion)
          .setSize(size)
          .build();
    }

    public static Builder builder() {
      return new AutoValue_ProduceResponse_ProduceResponseData.Builder();
    }

    @AutoValue.Builder
    public abstract static class Builder {

      Builder() {}

      public abstract Builder setType(Optional<EmbeddedFormat> type);

      public abstract Builder setType(@Nullable EmbeddedFormat type);

      public abstract Builder setSubject(Optional<String> subject);

      public abstract Builder setSubject(@Nullable String subject);

      public abstract Builder setSchemaId(Optional<Integer> schemaId);

      public abstract Builder setSchemaId(@Nullable Integer schemaId);

      public abstract Builder setSchemaVersion(Optional<Integer> schemaVersion);

      public abstract Builder setSchemaVersion(@Nullable Integer schemaVersion);

      public abstract Builder setSize(int size);

      public abstract ProduceResponseData build();
    }
  }
}
