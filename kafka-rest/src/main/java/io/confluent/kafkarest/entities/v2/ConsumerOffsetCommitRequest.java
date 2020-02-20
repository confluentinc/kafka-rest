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
import java.util.List;
import java.util.Objects;
import java.util.StringJoiner;
import javax.annotation.Nullable;

public final class ConsumerOffsetCommitRequest {

  @Nullable
  private final List<TopicPartitionOffsetMetadata> offsets;

  @JsonCreator
  public ConsumerOffsetCommitRequest(
      @JsonProperty("offsets") @Nullable List<TopicPartitionOffsetMetadata> offsets) {
    this.offsets = offsets;
  }

  @JsonProperty
  @Nullable
  public List<TopicPartitionOffsetMetadata> getOffsets() {
    return offsets;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ConsumerOffsetCommitRequest that = (ConsumerOffsetCommitRequest) o;
    return Objects.equals(offsets, that.offsets);
  }

  @Override
  public int hashCode() {
    return Objects.hash(offsets);
  }

  @Override
  public String toString() {
    return new StringJoiner(
        ", ", ConsumerOffsetCommitRequest.class.getSimpleName() + "[", "]")
        .add("offsets=" + offsets)
        .toString();
  }
}
