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
import jakarta.annotation.Nullable;
import java.util.Optional;

@AutoValue
public abstract class ProduceBatchResponseFailureEntry {

  @JsonCreator
  public ProduceBatchResponseFailureEntry() {}

  @JsonProperty("id")
  public abstract String getId();

  @JsonProperty("error_code")
  public abstract int getErrorCode();

  @JsonProperty("message")
  @JsonInclude(Include.NON_ABSENT)
  public abstract Optional<String> getMessage();

  @JsonCreator
  static ProduceBatchResponseFailureEntry fromJson(
      @JsonProperty("id") String id,
      @JsonProperty("error_code") int errorCode,
      @JsonProperty("message") Optional<String> message) {
    return builder().setId(id).setErrorCode(errorCode).setMessage(message).build();
  }

  public static Builder builder() {
    return new AutoValue_ProduceBatchResponseFailureEntry.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {

    Builder() {}

    public abstract Builder setId(String id);

    public abstract Builder setErrorCode(int errorCode);

    public abstract Builder setMessage(Optional<String> message);

    public abstract Builder setMessage(@Nullable String message);

    public abstract ProduceBatchResponseFailureEntry build();
  }
}
