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
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.auto.value.AutoValue;
import io.confluent.kafkarest.entities.v3.ProduceResponse.ProduceResponseData;
import jakarta.annotation.Nullable;
import java.time.Instant;
import java.util.Optional;

@AutoValue
public abstract class ProduceBatchResponseSuccessEntry {

  ProduceBatchResponseSuccessEntry() {}

  @JsonProperty("id")
  public abstract String getId();

  @JsonProperty("cluster_id")
  @JsonInclude(Include.NON_ABSENT)
  public abstract Optional<String> getClusterId();

  @JsonProperty("topic_name")
  @JsonInclude(Include.NON_ABSENT)
  public abstract Optional<String> getTopicName();

  @JsonProperty("partition_id")
  @JsonInclude(Include.NON_NULL)
  public abstract @Nullable Integer getPartitionId();

  @JsonProperty("offset")
  @JsonInclude(Include.NON_NULL)
  public abstract @Nullable Long getOffset();

  @JsonProperty("timestamp")
  @JsonInclude(Include.NON_ABSENT)
  public abstract Optional<Instant> getTimestamp();

  @JsonProperty("key")
  @JsonInclude(Include.NON_ABSENT)
  public abstract Optional<ProduceResponseData> getKey();

  @JsonProperty("value")
  @JsonInclude(Include.NON_ABSENT)
  public abstract Optional<ProduceResponseData> getValue();

  @JsonCreator
  static ProduceBatchResponseSuccessEntry fromJson(
      @JsonProperty("id") String id,
      @JsonProperty("cluster_id") @Nullable String clusterId,
      @JsonProperty("topic_name") @Nullable String topicName,
      @JsonProperty("partition_id") @Nullable Integer partitionId,
      @JsonProperty("offset") @Nullable Long offset,
      @JsonProperty("timestamp") @Nullable Instant timestamp,
      @JsonProperty("key") @Nullable ProduceResponseData key,
      @JsonProperty("value") @Nullable ProduceResponseData value) {
    return builder()
        .setId(id)
        .setClusterId(clusterId)
        .setTopicName(topicName)
        .setPartitionId(partitionId)
        .setOffset(offset)
        .setTimestamp(timestamp)
        .setKey(key)
        .setValue(value)
        .build();
  }

  public static Builder builder() {
    return new AutoValue_ProduceBatchResponseSuccessEntry.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {

    Builder() {}

    public abstract Builder setId(String id);

    public abstract Builder setClusterId(Optional<String> clusterId);

    public abstract Builder setClusterId(@Nullable String clusterId);

    public abstract Builder setTopicName(Optional<String> topicName);

    public abstract Builder setTopicName(@Nullable String topicName);

    public abstract Builder setPartitionId(@Nullable Integer partitionId);

    public abstract Builder setOffset(@Nullable Long offset);

    public abstract Builder setTimestamp(Optional<Instant> timestamp);

    public abstract Builder setTimestamp(@Nullable Instant timestamp);

    public abstract Builder setKey(Optional<ProduceResponseData> key);

    public abstract Builder setKey(@Nullable ProduceResponseData key);

    public abstract Builder setValue(Optional<ProduceResponseData> value);

    public abstract Builder setValue(@Nullable ProduceResponseData value);

    public abstract ProduceBatchResponseSuccessEntry build();
  }
}
