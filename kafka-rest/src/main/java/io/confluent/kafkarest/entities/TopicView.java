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

package io.confluent.kafkarest.entities;

import com.google.auto.value.AutoValue;
import jakarta.annotation.Nullable;

/**
 * Internal domain model for a topic view derived from a source topic. Independent of any
 * wire-format / API version. Mapped to the v3 REST DTO via {@code
 * io.confluent.kafkarest.entities.v3.TopicViewInfo#fromTopicView(TopicView)}.
 */
@AutoValue
public abstract class TopicView {

  TopicView() {}

  public abstract String getViewTopicName();

  public abstract String getViewTopicId();

  public abstract String getViewFilter();

  public abstract String getFlinkStatementId();

  public abstract String getFlinkComputePoolId();

  public abstract String getViewStatus();

  @Nullable
  public abstract String getViewStatusMessage();

  /** Epoch milliseconds at which the current {@link #getViewStatus()} was last set. */
  public abstract long getStatusChangedAt();

  public static Builder builder() {
    return new AutoValue_TopicView.Builder();
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

    public abstract TopicView build();
  }
}
