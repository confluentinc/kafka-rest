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
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.confluent.kafkarest.Errors;
import java.util.List;
import java.util.Objects;
import java.util.StringJoiner;
import javax.annotation.Nullable;
import javax.validation.constraints.NotEmpty;
import javax.ws.rs.core.Response;

public final class ProduceResponse {

  @NotEmpty
  @Nullable
  private final List<PartitionOffset> offsets;

  @Nullable
  private final Integer keySchemaId;

  @Nullable
  private final Integer valueSchemaId;

  @JsonCreator
  public ProduceResponse(
      @JsonProperty("offsets") @Nullable List<PartitionOffset> offsets,
      @JsonProperty("key_schema_id") @Nullable Integer keySchemaId,
      @JsonProperty("value_schema_id") @Nullable Integer valueSchemaId
  ) {
    this.offsets = offsets;
    this.keySchemaId = keySchemaId;
    this.valueSchemaId = valueSchemaId;
  }

  @JsonProperty
  @Nullable
  public List<PartitionOffset> getOffsets() {
    return offsets;
  }

  @JsonProperty("key_schema_id")
  @Nullable
  public Integer getKeySchemaId() {
    return keySchemaId;
  }

  @JsonProperty("value_schema_id")
  @Nullable
  public Integer getValueSchemaId() {
    return valueSchemaId;
  }

  @JsonIgnore
  public Response.Status getRequestStatus() {
    for (PartitionOffset partitionOffset : offsets) {
      if (partitionOffset.getErrorCode() == null) {
        continue;
      }

      if (partitionOffset.getErrorCode() == Errors.KAFKA_AUTHENTICATION_ERROR_CODE) {
        return Response.Status.UNAUTHORIZED;
      } else if (partitionOffset.getErrorCode() == Errors.KAFKA_AUTHORIZATION_ERROR_CODE) {
        return Response.Status.FORBIDDEN;
      }
    }
    return Response.Status.OK;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ProduceResponse that = (ProduceResponse) o;
    return Objects.equals(offsets, that.offsets)
        && Objects.equals(keySchemaId, that.keySchemaId)
        && Objects.equals(valueSchemaId, that.valueSchemaId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(offsets, keySchemaId, valueSchemaId);
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", ProduceResponse.class.getSimpleName() + "[", "]")
        .add("offsets=" + offsets)
        .add("keySchemaId=" + keySchemaId)
        .add("valueSchemaId=" + valueSchemaId)
        .toString();
  }
}
