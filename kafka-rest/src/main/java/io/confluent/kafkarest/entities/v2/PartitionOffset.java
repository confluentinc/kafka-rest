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
import java.util.Objects;
import java.util.StringJoiner;
import javax.annotation.Nullable;
import javax.validation.constraints.PositiveOrZero;

public final class PartitionOffset {

  @PositiveOrZero
  @Nullable
  private final Integer partition;

  @PositiveOrZero
  @Nullable
  private final Long offset;

  @Nullable
  private final Integer errorCode;

  @Nullable
  private final String error;

  @JsonCreator
  public PartitionOffset(
      @JsonProperty("partition") @Nullable Integer partition,
      @JsonProperty("offset") @Nullable Long offset,
      @JsonProperty("error_code") @Nullable Integer errorCode,
      @JsonProperty("error") @Nullable String error
  ) {
    this.partition = partition;
    this.offset = offset;
    this.errorCode = errorCode;
    this.error = error;
  }

  @JsonProperty
  @Nullable
  public Integer getPartition() {
    return partition;
  }

  @JsonProperty
  @Nullable
  public Long getOffset() {
    return offset;
  }

  @JsonProperty("error_code")
  @Nullable
  public Integer getErrorCode() {
    return errorCode;
  }

  @JsonProperty("error")
  @Nullable
  public String getError() {
    return error;
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
    return Objects.equals(partition, that.partition)
        && Objects.equals(offset, that.offset)
        && Objects.equals(errorCode, that.errorCode)
        && Objects.equals(error, that.error);
  }

  @Override
  public int hashCode() {
    return Objects.hash(partition, offset, errorCode, error);
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", PartitionOffset.class.getSimpleName() + "[", "]")
        .add("partition=" + partition)
        .add("offset=" + offset)
        .add("errorCode=" + errorCode)
        .add("error='" + error + "'")
        .toString();
  }
}
