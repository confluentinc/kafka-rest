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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.confluent.kafkarest.Errors;
import java.util.List;
import javax.ws.rs.core.Response;
import org.hibernate.validator.constraints.NotEmpty;

public class ProduceResponse {

  @NotEmpty
  private List<PartitionOffset> offsets;

  private Integer keySchemaId;

  private Integer valueSchemaId;

  @JsonProperty
  public List<PartitionOffset> getOffsets() {
    return offsets;
  }

  @JsonProperty
  public void setOffsets(List<PartitionOffset> offsets) {
    this.offsets = offsets;
  }

  @JsonProperty("key_schema_id")
  public Integer getKeySchemaId() {
    return keySchemaId;
  }

  public void setKeySchemaId(Integer keySchemaId) {
    this.keySchemaId = keySchemaId;
  }

  @JsonProperty("value_schema_id")
  public Integer getValueSchemaId() {
    return valueSchemaId;
  }

  public void setValueSchemaId(Integer valueSchemaId) {
    this.valueSchemaId = valueSchemaId;
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
  public String toString() {
    return "ProduceResponse{"
           + "offsets=" + offsets
           + ", keySchemaId=" + keySchemaId
           + ", valueSchemaId=" + valueSchemaId
           + '}';
  }
}
