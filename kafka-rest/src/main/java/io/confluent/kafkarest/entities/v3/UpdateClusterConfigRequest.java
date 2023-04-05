/*
 * Copyright 2020 Confluent Inc.
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
import java.util.Optional;
import javax.annotation.Nullable;

@AutoValue
public abstract class UpdateClusterConfigRequest {

  UpdateClusterConfigRequest() {
  }

  @JsonProperty("value")
  public abstract Optional<String> getValue();

  public static UpdateClusterConfigRequest create(@Nullable String value) {
    return new AutoValue_UpdateClusterConfigRequest(Optional.ofNullable(value));
  }

  @JsonCreator
  static UpdateClusterConfigRequest fromJson(@JsonProperty("value") @Nullable String value) {
    return create(value);
  }
}
