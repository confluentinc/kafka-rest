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
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.constraints.Min;

public class PartitionOffset {

  @Min(0)
  private Integer partition;
  @Min(0)
  private Long offset;

  private Integer errorCode;
  private String error;

  @JsonCreator
  public PartitionOffset(
      @JsonProperty("partition") Integer partition,
      @JsonProperty("offset") Long offset,
      @JsonProperty("error_code") Integer errorCode,
      @JsonProperty("error") String error
  ) {
    this.partition = partition;
    this.offset = offset;
    this.errorCode = errorCode;
    this.error = error;
  }

  @JsonProperty
  public Integer getPartition() {
    return partition;
  }

  public void setPartition(Integer partition) {
    this.partition = partition;
  }

  @JsonProperty
  public Long getOffset() {
    return offset;
  }

  public void setOffset(Long offset) {
    this.offset = offset;
  }

  @JsonProperty("error_code")
  public Integer getErrorCode() {
    return errorCode;
  }

  public void setErrorCode(Integer errorCode) {
    this.errorCode = errorCode;
  }

  @JsonProperty("error")
  public String getError() {
    return error;
  }

  public void setError(String error) {
    this.error = error;
  }

  @Override
  public String toString() {
    return "PartitionOffset{"
           + "partition=" + partition
           + ", offset=" + offset
           + ", errorCode=" + errorCode
           + ", error='" + error + '\''
           + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    PartitionOffset that = (PartitionOffset) o;

    if (error != null ? !error.equals(that.error) : that.error != null) {
      return false;
    }
    if (errorCode != null ? !errorCode.equals(that.errorCode) : that.errorCode != null) {
      return false;
    }
    if (offset != null ? !offset.equals(that.offset) : that.offset != null) {
      return false;
    }
    if (partition != null ? !partition.equals(that.partition) : that.partition != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int result = partition != null ? partition.hashCode() : 0;
    result = 31 * result + (offset != null ? offset.hashCode() : 0);
    result = 31 * result + (errorCode != null ? errorCode.hashCode() : 0);
    result = 31 * result + (error != null ? error.hashCode() : 0);
    return result;
  }
}
