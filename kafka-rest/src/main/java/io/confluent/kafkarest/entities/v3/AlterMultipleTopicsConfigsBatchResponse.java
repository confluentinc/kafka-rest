/*
 * Copyright 2026 Confluent Inc.
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

package io.confluent.kafkarest.entities.v3;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableList;
import jakarta.annotation.Nullable;
import java.util.List;
import java.util.Optional;

@AutoValue
public abstract class AlterMultipleTopicsConfigsBatchResponse {

  AlterMultipleTopicsConfigsBatchResponse() {}

  @JsonProperty("failures")
  public abstract ImmutableList<FailureEntry> getFailures();

  public static AlterMultipleTopicsConfigsBatchResponse create(List<FailureEntry> failures) {
    return new AutoValue_AlterMultipleTopicsConfigsBatchResponse(ImmutableList.copyOf(failures));
  }

  @JsonCreator
  static AlterMultipleTopicsConfigsBatchResponse fromJson(
      @JsonProperty("failures") List<FailureEntry> failures) {
    return create(failures != null ? failures : ImmutableList.of());
  }

  @AutoValue
  public abstract static class FailureEntry {

    FailureEntry() {}

    @JsonProperty("topic_name")
    public abstract String getTopicName();

    @JsonProperty("error_code")
    public abstract int getErrorCode();

    @JsonProperty("message")
    @JsonInclude(Include.NON_ABSENT)
    public abstract Optional<String> getMessage();

    public static FailureEntry create(String topicName, int errorCode, @Nullable String message) {
      return new AutoValue_AlterMultipleTopicsConfigsBatchResponse_FailureEntry(
          topicName, errorCode, Optional.ofNullable(message));
    }

    @JsonCreator
    static FailureEntry fromJson(
        @JsonProperty("topic_name") String topicName,
        @JsonProperty("error_code") int errorCode,
        @JsonProperty("message") Optional<String> message) {
      return new AutoValue_AlterMultipleTopicsConfigsBatchResponse_FailureEntry(
          topicName, errorCode, message);
    }
  }
}
