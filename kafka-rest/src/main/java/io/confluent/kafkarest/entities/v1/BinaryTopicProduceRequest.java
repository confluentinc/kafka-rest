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
import io.confluent.kafkarest.entities.EntityUtils;
import io.confluent.kafkarest.entities.ProduceRecord;
import io.confluent.kafkarest.entities.ProduceRequest;
import io.confluent.rest.validation.ConstraintViolations;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.PositiveOrZero;

public final class BinaryTopicProduceRequest {

  @NotEmpty
  @Nullable
  private final List<BinaryTopicProduceRecord> records;

  @JsonCreator
  private BinaryTopicProduceRequest(
      @JsonProperty("records") @Nullable List<BinaryTopicProduceRecord> records,
      @JsonProperty("key_schema") @Nullable String keySchema,
      @JsonProperty("key_schema_id") @Nullable Integer keySchemaId,
      @JsonProperty("value_schema") @Nullable String valueSchema,
      @JsonProperty("value_schema_id") @Nullable Integer valueSchemaId
  ) {
    this.records = records;
  }

  @JsonProperty("records")
  @Nullable
  public List<BinaryTopicProduceRecord> getRecords() {
    return records;
  }

  public static BinaryTopicProduceRequest create(List<BinaryTopicProduceRecord> records) {
    if (records.isEmpty()) {
      throw new IllegalArgumentException();
    }
    return new BinaryTopicProduceRequest(
        records,
        /* keySchema= */ null,
        /* keySchemaId= */ null,
        /* valueSchema= */ null,
        /* valueSchemaId= */ null);
  }

  public ProduceRequest<byte[], byte[]> toProduceRequest() {
    if (records == null || records.isEmpty()) {
      throw new IllegalStateException();
    }
    return new ProduceRequest<>(
        records.stream()
            .map(record -> new ProduceRecord<>(record.key, record.value, record.partition))
            .collect(Collectors.toList()),
        /* keySchema= */ null,
        /* keySchemaId= */ null,
        /* valueSchema= */ null,
        /* valueSchemaId= */ null);
  }

  public static final class BinaryTopicProduceRecord {

    @Nullable
    private final byte[] key;

    @Nullable
    private final byte[] value;

    @PositiveOrZero
    @Nullable
    private final Integer partition;

    @JsonCreator
    public BinaryTopicProduceRecord(
        @JsonProperty("key") @Nullable String key,
        @JsonProperty("value") @Nullable String value,
        @JsonProperty("partition") @Nullable Integer partition
    ) {
      try {
        this.key = (key != null) ? EntityUtils.parseBase64Binary(key) : null;
      } catch (IllegalArgumentException e) {
        throw ConstraintViolations.simpleException("Record key contains invalid base64 encoding");
      }
      try {
        this.value = (value != null) ? EntityUtils.parseBase64Binary(value) : null;
      } catch (IllegalArgumentException e) {
        throw ConstraintViolations.simpleException("Record value contains invalid base64 encoding");
      }
      this.partition = partition;
    }

    @JsonProperty("key")
    @Nullable
    public String getKey() {
      return (key == null ? null : EntityUtils.encodeBase64Binary(key));
    }

    @JsonProperty("value")
    @Nullable
    public String getValue() {
      return (value == null ? null : EntityUtils.encodeBase64Binary(value));
    }

    @JsonProperty("partition")
    @Nullable
    public Integer getPartition() {
      return partition;
    }
  }
}
