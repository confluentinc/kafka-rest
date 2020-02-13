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

package io.confluent.kafkarest.entities.v1;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import io.confluent.kafkarest.entities.ProduceRecord;
import io.confluent.kafkarest.entities.ProduceRequest;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.validation.constraints.NotEmpty;

public final class AvroPartitionProduceRequest {

  @NotEmpty
  @Nullable
  private final List<AvroPartitionProduceRecord> records;

  @Nullable
  private final String keySchema;

  @Nullable
  private final Integer keySchemaId;

  @Nullable
  private final String valueSchema;

  @Nullable
  private final Integer valueSchemaId;

  @JsonCreator
  private AvroPartitionProduceRequest(
      @JsonProperty("records") @Nullable List<AvroPartitionProduceRecord> records,
      @JsonProperty("key_schema") @Nullable String keySchema,
      @JsonProperty("key_schema_id") @Nullable Integer keySchemaId,
      @JsonProperty("value_schema") @Nullable String valueSchema,
      @JsonProperty("value_schema_id") @Nullable Integer valueSchemaId
  ) {
    this.records = records;
    this.keySchema = keySchema;
    this.keySchemaId = keySchemaId;
    this.valueSchema = valueSchema;
    this.valueSchemaId = valueSchemaId;
  }

  @JsonProperty("records")
  @Nullable
  public List<AvroPartitionProduceRecord> getRecords() {
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

  public static AvroPartitionProduceRequest create(
      List<AvroPartitionProduceRecord> records,
      @Nullable String keySchema,
      @Nullable Integer keySchemaId,
      @Nullable String valueSchema,
      @Nullable Integer valueSchemaId) {
    if (records.isEmpty()) {
      throw new IllegalArgumentException();
    }
    return new AvroPartitionProduceRequest(
        records, keySchema, keySchemaId, valueSchema, valueSchemaId);
  }
  
  public ProduceRequest<JsonNode, JsonNode> toProduceRequest() {
    if (records == null || records.isEmpty()) {
      throw new IllegalStateException();
    }
    return new ProduceRequest<>(
        records.stream()
            .map(record -> new ProduceRecord<>(record.key, record.value, null))
            .collect(Collectors.toList()),
        keySchema,
        keySchemaId,
        valueSchema,
        valueSchemaId);
  }

  public static final class AvroPartitionProduceRecord {

    @Nullable
    private final JsonNode key;

    @Nullable
    private final JsonNode value;

    @JsonCreator
    public AvroPartitionProduceRecord(
        @JsonProperty("key") @Nullable JsonNode key,
        @JsonProperty("value") @Nullable JsonNode value
    ) {
      this.key = key;
      this.value = value;
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
  }
}
