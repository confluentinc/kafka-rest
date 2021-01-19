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

package io.confluent.kafkarest.entities.v2;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nullable;
import javax.validation.constraints.NotEmpty;

@AutoValue
public abstract class ProduceRequest {

  @NotEmpty
  @JsonProperty("records")
  public abstract ImmutableList<ProduceRecord> getRecords();

  @JsonProperty("key_schema_id")
  public abstract Optional<Integer> getKeySchemaId();

  @JsonProperty("key_schema")
  public abstract Optional<String> getKeySchema();

  @JsonProperty("value_schema_id")
  public abstract Optional<Integer> getValueSchemaId();

  @JsonProperty("value_schema")
  public abstract Optional<String> getValueSchema();

  public static ProduceRequest create(List<ProduceRecord> records) {
    return create(
        records,
        /* keySchemaId= */ null,
        /* keySchema= */ null,
        /* valueSchemaId= */ null,
        /* valueSchema= */ null);
  }

  public static ProduceRequest create(
      List<ProduceRecord> records,
      @Nullable Integer keySchemaId,
      @Nullable String keySchema,
      @Nullable Integer valueSchemaId,
      @Nullable String valueSchema) {
    return new AutoValue_ProduceRequest(
        ImmutableList.copyOf(records),
        Optional.ofNullable(keySchemaId),
        Optional.ofNullable(keySchema),
        Optional.ofNullable(valueSchemaId),
        Optional.ofNullable(valueSchema));
  }

  @JsonCreator
  static ProduceRequest fromJson(
      @JsonProperty("records") @Nullable List<ProduceRecord> records,
      @JsonProperty("key_schema_id") @Nullable Integer keySchemaId,
      @JsonProperty("key_schema") @Nullable String keySchema,
      @JsonProperty("value_schema_id") @Nullable Integer valueSchemaId,
      @JsonProperty("value_schema") @Nullable String valueSchema) {
    return create(
        records != null ? records : ImmutableList.of(),
        keySchemaId,
        keySchema,
        valueSchemaId,
        valueSchema);
  }

  @AutoValue
  public abstract static class ProduceRecord {

    @JsonProperty("partition")
    public abstract Optional<Integer> getPartition();

    @JsonProperty("key")
    public abstract Optional<JsonNode> getKey();

    @JsonProperty("value")
    public abstract Optional<JsonNode> getValue();

    public static ProduceRecord create(@Nullable JsonNode key, @Nullable JsonNode value) {
      return create(/* partition= */ null, key, value);
    }

    public static ProduceRecord create(
        @Nullable Integer partition, @Nullable JsonNode key, @Nullable JsonNode value) {
      return new AutoValue_ProduceRequest_ProduceRecord(
          Optional.ofNullable(partition), Optional.ofNullable(key), Optional.ofNullable(value));
    }

    @JsonCreator
    static ProduceRecord fromJson(
        @JsonProperty("partition") @Nullable Integer partition,
        @JsonProperty("key") @Nullable JsonNode key,
        @JsonProperty("value") @Nullable JsonNode value) {
      return create(partition, key, value);
    }
  }
}
