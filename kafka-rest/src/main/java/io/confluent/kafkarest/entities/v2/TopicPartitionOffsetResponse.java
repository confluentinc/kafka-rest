/*
 * Copyright 2019 Confluent Inc.
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
import com.fasterxml.jackson.annotation.JsonProperty.Access;
import java.util.Objects;
import java.util.StringJoiner;
import javax.annotation.Nullable;

/**
 * Response for GET /topics/(topic)/partitions/(partition)/offsets requests.
 *
 * @see io.confluent.kafkarest.resources.v2.PartitionsResource#getOffsets(String, int)
 */
public final class TopicPartitionOffsetResponse {

  @Nullable
  private final Long beginningOffset;

  @Nullable
  private final Long endOffset;

  @JsonCreator
  public TopicPartitionOffsetResponse(
      @JsonProperty("beginning_offset") @Nullable Long beginningOffset,
      @JsonProperty("end_offset") @Nullable Long endOffset
  ) {
    this.beginningOffset = beginningOffset;
    this.endOffset = endOffset;
  }

  /**
   * The earliest offset in the topic partition.
   */
  @JsonProperty(value = "beginning_offset", access = Access.READ_ONLY)
  @Nullable
  public Long getBeginningOffset() {
    return beginningOffset;
  }

  /**
   * The latest offset in the topic partition.
   */
  @JsonProperty(value = "end_offset", access = Access.READ_ONLY)
  @Nullable
  public Long getEndOffset() {
    return endOffset;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    TopicPartitionOffsetResponse that = (TopicPartitionOffsetResponse) o;
    return Objects.equals(beginningOffset, that.beginningOffset)
        && Objects.equals(endOffset, that.endOffset);
  }

  @Override
  public int hashCode() {
    return Objects.hash(beginningOffset, endOffset);
  }

  @Override
  public String toString() {
    return new StringJoiner(
        ", ", TopicPartitionOffsetResponse.class.getSimpleName() + "[", "]")
        .add("beginningOffset=" + beginningOffset)
        .add("endOffset=" + endOffset)
        .toString();
  }
}
