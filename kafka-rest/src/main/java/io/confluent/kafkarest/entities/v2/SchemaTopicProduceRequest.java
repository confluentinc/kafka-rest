/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.kafkarest.entities.v2;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import io.confluent.kafkarest.entities.ProduceRecord;
import io.confluent.kafkarest.entities.ProduceRequest;
import jakarta.annotation.Nullable;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.PositiveOrZero;
import java.util.List;
import java.util.Objects;
import java.util.StringJoiner;
import java.util.stream.Collectors;

public final class SchemaTopicProduceRequest {

  @NotEmpty @Nullable private final List<SchemaTopicProduceRecord> records;

  @Nullable private final String keySchema;

  @Nullable private final Integer keySchemaId;

  @Nullable private final String valueSchema;

  @Nullable private final Integer valueSchemaId;

  @JsonCreator
  public SchemaTopicProduceRequest(
      @JsonProperty("records") @Nullable List<SchemaTopicProduceRecord> records,
      @JsonProperty("key_schema") @Nullable String keySchema,
      @JsonProperty("key_schema_id") @Nullable Integer keySchemaId,
      @JsonProperty("value_schema") @Nullable String valueSchema,
      @JsonProperty("value_schema_id") @Nullable Integer valueSchemaId) {
    this.records = records;
    this.keySchema = keySchema;
    this.keySchemaId = keySchemaId;
    this.valueSchema = valueSchema;
    this.valueSchemaId = valueSchemaId;
  }

  @JsonProperty("records")
  @Nullable
  public List<SchemaTopicProduceRecord> getRecords() {
    return records;
  }

  @JsonProperty("key_schema")
  @Nullable
  public String getKeySchema() {
    return keySchema;
  }

  @JsonProperty("key_schema_id")
  @Nullable
  public Integer getKeySchemaId() {
    return keySchemaId;
  }

  @JsonProperty("value_schema")
  @Nullable
  public String getValueSchema() {
    return valueSchema;
  }

  @JsonProperty("value_schema_id")
  @Nullable
  public Integer getValueSchemaId() {
    return valueSchemaId;
  }

  public static SchemaTopicProduceRequest create(
      List<SchemaTopicProduceRecord> records,
      @Nullable String keySchema,
      @Nullable Integer keySchemaId,
      @Nullable String valueSchema,
      @Nullable Integer valueSchemaId) {
    if (records.isEmpty()) {
      throw new IllegalArgumentException();
    }
    return new SchemaTopicProduceRequest(
        records, keySchema, keySchemaId, valueSchema, valueSchemaId);
  }

  public ProduceRequest<JsonNode, JsonNode> toProduceRequest() {
    if (records == null || records.isEmpty()) {
      throw new IllegalStateException();
    }
    return ProduceRequest.create(
        records.stream()
            .map(record -> ProduceRecord.create(record.key, record.value, record.partition))
            .collect(Collectors.toList()),
        keySchema,
        keySchemaId,
        valueSchema,
        valueSchemaId);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SchemaTopicProduceRequest that = (SchemaTopicProduceRequest) o;
    return Objects.equals(records, that.records)
        && Objects.equals(keySchema, that.keySchema)
        && Objects.equals(keySchemaId, that.keySchemaId)
        && Objects.equals(valueSchema, that.valueSchema)
        && Objects.equals(valueSchemaId, that.valueSchemaId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(records, keySchema, keySchemaId, valueSchema, valueSchemaId);
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", SchemaTopicProduceRequest.class.getSimpleName() + "[", "]")
        .add("records=" + records)
        .add("keySchema='" + keySchema + "'")
        .add("keySchemaId=" + keySchemaId)
        .add("valueSchema='" + valueSchema + "'")
        .add("valueSchemaId=" + valueSchemaId)
        .toString();
  }

  public static final class SchemaTopicProduceRecord {

    @Nullable private final JsonNode key;

    @Nullable private final JsonNode value;

    @PositiveOrZero @Nullable private final Integer partition;

    @JsonCreator
    public SchemaTopicProduceRecord(
        @JsonProperty("key") @Nullable JsonNode key,
        @JsonProperty("value") @Nullable JsonNode value,
        @JsonProperty("partition") @Nullable Integer partition) {

      this.key = key;
      this.value = value;
      this.partition = partition;
    }

    @JsonProperty("key")
    @Nullable
    public JsonNode getKey() {
      return key;
    }

    @JsonProperty("value")
    @Nullable
    public JsonNode getValue() {
      return value;
    }

    @JsonProperty("partition")
    @Nullable
    public Integer getPartition() {
      return partition;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      SchemaTopicProduceRecord that = (SchemaTopicProduceRecord) o;
      return Objects.equals(key, that.key)
          && Objects.equals(value, that.value)
          && Objects.equals(partition, that.partition);
    }

    @Override
    public int hashCode() {
      return Objects.hash(key, value, partition);
    }

    @Override
    public String toString() {
      return new StringJoiner(", ", SchemaTopicProduceRecord.class.getSimpleName() + "[", "]")
          .add("key=" + key)
          .add("value=" + value)
          .add("partition=" + partition)
          .toString();
    }
  }
}
