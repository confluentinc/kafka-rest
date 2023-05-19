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
import java.util.Optional;

@AutoValue
public abstract class ProduceBatchErrorEntry {

  @JsonCreator
  public ProduceBatchErrorEntry() {}

  @JsonProperty("id")
  public abstract String getId();

  @JsonProperty("error_code")
  public abstract int getErrorCode();

  @JsonProperty("message")
  @JsonInclude(Include.NON_ABSENT)
  public abstract Optional<String> getMessage();

  public static Builder builder() {
    return new AutoValue_ProduceBatchErrorEntry.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {

    Builder() {}

    public abstract Builder setId(String id);

    public abstract Builder setErrorCode(int errorCode);

    public abstract Builder setMessage(String message);

    public abstract ProduceBatchErrorEntry build();
  }
}
