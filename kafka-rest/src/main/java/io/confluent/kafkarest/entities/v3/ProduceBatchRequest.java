/*
 * Copyright 2023 Confluent Inc.
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
import com.google.common.collect.ImmutableList;
import jakarta.annotation.Nullable;
import java.util.List;

@AutoValue
public abstract class ProduceBatchRequest {

  ProduceBatchRequest() {}

  @JsonProperty("entries")
  public abstract ImmutableList<ProduceBatchRequestEntry> getEntries();

  public static Builder builder() {
    return new AutoValue_ProduceBatchRequest.Builder();
  }

  @JsonCreator
  static ProduceBatchRequest fromJson(
      @JsonProperty("entries") @Nullable List<ProduceBatchRequestEntry> entries) {
    return builder()
        .setEntries(entries != null ? ImmutableList.copyOf(entries) : ImmutableList.of())
        .build();
  }

  @AutoValue.Builder
  public abstract static class Builder {

    Builder() {}

    public abstract Builder setEntries(List<ProduceBatchRequestEntry> entries);

    public abstract ProduceBatchRequest build();
  }
}
