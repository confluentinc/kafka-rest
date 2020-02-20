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

package io.confluent.kafkarest.entities;

import java.util.List;
import java.util.Objects;
import java.util.StringJoiner;
import javax.annotation.Nullable;

public final class ProduceRequest<K, V> {

  private final List<ProduceRecord<K, V>> records;

  @Nullable
  private final String keySchema;

  @Nullable
  private final Integer keySchemaId;

  @Nullable
  private final String valueSchema;

  @Nullable
  private final Integer valueSchemaId;

  public ProduceRequest(
      List<ProduceRecord<K, V>> records,
      @Nullable String keySchema,
      @Nullable Integer keySchemaId,
      @Nullable String valueSchema,
      @Nullable Integer valueSchemaId) {
    if (records.isEmpty()) {
      throw new IllegalStateException();
    }
    this.records = records;
    this.keySchema = keySchema;
    this.keySchemaId = keySchemaId;
    this.valueSchema = valueSchema;
    this.valueSchemaId = valueSchemaId;
  }

  public List<ProduceRecord<K, V>> getRecords() {
    return records;
  }

  @Nullable
  public String getKeySchema() {
    return keySchema;
  }

  @Nullable
  public Integer getKeySchemaId() {
    return keySchemaId;
  }

  @Nullable
  public String getValueSchema() {
    return valueSchema;
  }

  @Nullable
  public Integer getValueSchemaId() {
    return valueSchemaId;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ProduceRequest<?, ?> that = (ProduceRequest<?, ?>) o;
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
    return new StringJoiner(", ", ProduceRequest.class.getSimpleName() + "[", "]")
        .add("records=" + records)
        .add("keySchema='" + keySchema + "'")
        .add("keySchemaId=" + keySchemaId)
        .add("valueSchema='" + valueSchema + "'")
        .add("valueSchemaId=" + valueSchemaId)
        .toString();
  }
}
