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

import com.google.auto.value.AutoValue;
import java.util.List;
import javax.annotation.Nullable;

@AutoValue
public abstract class ProduceRequest<K, V> {

  ProduceRequest() {
  }

  public abstract List<ProduceRecord<K, V>> getRecords();

  @Nullable
  public abstract String getKeySchema();

  @Nullable
  public abstract Integer getKeySchemaId();

  @Nullable
  public abstract String getValueSchema();

  @Nullable
  public abstract Integer getValueSchemaId();

  public static <K, V> ProduceRequest<K, V> create(
      List<ProduceRecord<K, V>> records,
      @Nullable String keySchema,
      @Nullable Integer keySchemaId,
      @Nullable String valueSchema,
      @Nullable Integer valueSchemaId) {
    if (records.isEmpty()) {
      throw new IllegalStateException();
    }
    return new AutoValue_ProduceRequest<>(
        records, keySchema, keySchemaId, valueSchema, valueSchemaId);
  }
}
