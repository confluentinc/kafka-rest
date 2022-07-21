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
import io.confluent.kafkarest.entities.ProduceRecord;
import io.confluent.kafkarest.entities.ProduceRequest;
import java.util.List;
import java.util.Objects;
import java.util.StringJoiner;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.PositiveOrZero;

public final class JsonTopicProduceRequest {

  @NotEmpty
  @Nullable
  private final List<JsonTopicProduceRecord> records;

  @JsonCreator
  private JsonTopicProduceRequest(
      @JsonProperty("records") @Nullable List<JsonTopicProduceRecord> records,
      @JsonProperty("key_schema") @Nullable String keySchema,
      @JsonProperty("key_schema_id") @Nullable Integer keySchemaId,
      @JsonProperty("value_schema") @Nullable String valueSchema,
      @JsonProperty("value_schema_id") @Nullable Integer valueSchemaId
  ) {
    this.records = records;
  }

  @JsonProperty("records")
  @Nullable
  public List<JsonTopicProduceRecord> getRecords() {
    return records;
  }

  public static JsonTopicProduceRequest create(List<JsonTopicProduceRecord> records) {
    if (records.isEmpty()) {
      throw new IllegalArgumentException();
    }
    return new JsonTopicProduceRequest(
        records,
        /* keySchema= */ null,
        /* keySchemaId= */ null,
        /* valueSchema= */ null,
        /* valueSchemaId= */ null);
  }

  public ProduceRequest<Object, Object> toProduceRequest() {
    if (records == null || records.isEmpty()) {
      throw new IllegalStateException();
    }
    return ProduceRequest.create(
        records.stream()
            .map(record -> ProduceRecord.create(record.key, record.value, record.partition))
            .collect(Collectors.toList()),
        /* keySchema= */ null,
        /* keySchemaId= */ null,
        /* valueSchema= */ null,
        /* valueSchemaId= */ null);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    JsonTopicProduceRequest that = (JsonTopicProduceRequest) o;
    return Objects.equals(records, that.records);
  }

  @Override
  public int hashCode() {
    return Objects.hash(records);
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", JsonTopicProduceRequest.class.getSimpleName() + "[", "]")
        .add("records=" + records)
        .toString();
  }

  public static final class JsonTopicProduceRecord {

    @Nullable
    private final Object key;

    @Nullable
    private final Object value;

    @PositiveOrZero
    @Nullable
    private final Integer partition;

    @JsonCreator
    public JsonTopicProduceRecord(
        @JsonProperty("key") @Nullable Object key,
        @JsonProperty("value") @Nullable Object value,
        @JsonProperty("partition") @Nullable Integer partition
    ) {
      this.key = key;
      this.value = value;
      this.partition = partition;
    }

    @JsonProperty("key")
    @Nullable
    public Object getKey() {
      return key;
    }

    @JsonProperty("value")
    @Nullable
    public Object getValue() {
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
      JsonTopicProduceRecord that = (JsonTopicProduceRecord) o;
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
      return new StringJoiner(", ", JsonTopicProduceRecord.class.getSimpleName() + "[", "]")
          .add("key=" + key)
          .add("value=" + value)
          .add("partition=" + partition)
          .toString();
    }
  }
}
