/**
 * Copyright 2015 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.kafkarest.entities;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.constraints.Min;

public class PartitionProduceResponse {

  @Min(0)
  private int partition;
  @Min(0)
  private long offset;
  private Integer keySchemaId;
  private Integer valueSchemaId;

  @JsonCreator
  public PartitionProduceResponse(@JsonProperty("partition") int partition,
                                  @JsonProperty("offset") long offset,
                                  @JsonProperty("key_schema_id") Integer keySchemaId,
                                  @JsonProperty("value_schema_id") Integer valueSchemaId) {
    this.partition = partition;
    this.offset = offset;
    this.keySchemaId = keySchemaId;
    this.valueSchemaId = valueSchemaId;
  }

  @JsonProperty
  public int getPartition() {
    return partition;
  }

  public void setPartition(int partition) {
    this.partition = partition;
  }

  @JsonProperty
  public long getOffset() {
    return offset;
  }

  public void setOffset(long offset) {
    this.offset = offset;
  }

  @JsonIgnore
  public PartitionOffset getPartitionOffset() {
    return new PartitionOffset(partition, offset);
  }

  @JsonProperty("key_schema_id")
  public Integer getKeySchemaId() {
    return keySchemaId;
  }

  @JsonProperty("value_schema_id")
  public Integer getValueSchemaId() {
    return valueSchemaId;
  }
}
