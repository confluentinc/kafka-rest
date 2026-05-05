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
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.auto.value.AutoValue;
import io.confluent.kafkarest.entities.TopicView;
import jakarta.annotation.Nullable;

@AutoValue
public abstract class TopicViewInfo {

  TopicViewInfo() {}

  @JsonProperty("view_topic_name")
  public abstract String getViewTopicName();

  @JsonProperty("view_topic_id")
  public abstract String getViewTopicId();

  @JsonProperty("view_filter")
  public abstract String getViewFilter();

  @JsonProperty("flink_statement_id")
  public abstract String getFlinkStatementId();

  @JsonProperty("flink_compute_pool_id")
  public abstract String getFlinkComputePoolId();

  @JsonProperty("view_status")
  public abstract String getViewStatus();

  @JsonProperty("view_status_message")
  @Nullable
  public abstract String getViewStatusMessage();

  @JsonProperty("status_changed_at")
  public abstract long getStatusChangedAt();

  public static Builder builder() {
    return new AutoValue_TopicViewInfo.Builder();
  }

  /** Maps the internal {@link TopicView} domain entity to its v3 wire-format DTO. */
  public static TopicViewInfo fromTopicView(TopicView view) {
    return builder()
        .setViewTopicName(view.getViewTopicName())
        .setViewTopicId(view.getViewTopicId())
        .setViewFilter(view.getViewFilter())
        .setFlinkStatementId(view.getFlinkStatementId())
        .setFlinkComputePoolId(view.getFlinkComputePoolId())
        .setViewStatus(view.getViewStatus())
        .setViewStatusMessage(view.getViewStatusMessage())
        .setStatusChangedAt(view.getStatusChangedAt())
        .build();
  }

  @JsonCreator
  static TopicViewInfo fromJson(
      @JsonProperty("view_topic_name") String viewTopicName,
      @JsonProperty("view_topic_id") String viewTopicId,
      @JsonProperty("view_filter") String viewFilter,
      @JsonProperty("flink_statement_id") String flinkStatementId,
      @JsonProperty("flink_compute_pool_id") String flinkComputePoolId,
      @JsonProperty("view_status") String viewStatus,
      @JsonProperty("view_status_message") @Nullable String viewStatusMessage,
      @JsonProperty("status_changed_at") long statusChangedAt) {
    return builder()
        .setViewTopicName(viewTopicName)
        .setViewTopicId(viewTopicId)
        .setViewFilter(viewFilter)
        .setFlinkStatementId(flinkStatementId)
        .setFlinkComputePoolId(flinkComputePoolId)
        .setViewStatus(viewStatus)
        .setViewStatusMessage(viewStatusMessage)
        .setStatusChangedAt(statusChangedAt)
        .build();
  }

  @AutoValue.Builder
  public abstract static class Builder {

    Builder() {}

    public abstract Builder setViewTopicName(String viewTopicName);

    public abstract Builder setViewTopicId(String viewTopicId);

    public abstract Builder setViewFilter(String viewFilter);

    public abstract Builder setFlinkStatementId(String flinkStatementId);

    public abstract Builder setFlinkComputePoolId(String flinkComputePoolId);

    public abstract Builder setViewStatus(String viewStatus);

    public abstract Builder setViewStatusMessage(@Nullable String viewStatusMessage);

    public abstract Builder setStatusChangedAt(long statusChangedAt);

    public abstract TopicViewInfo build();
  }
}
