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

@AutoValue
public abstract class PartitionCountRequest {

  PartitionCountRequest() {}

  @JsonProperty("partition_count")
  public abstract Integer getPartitionCount();

  public static Builder builder() {
    return new AutoValue_PartitionCountRequest.Builder();
  }

  @JsonCreator
  static PartitionCountRequest fromJson(@JsonProperty("partition_count") Integer partitionCount) {
    return builder().setPartitionCount(partitionCount).build();
  }

  @AutoValue.Builder
  public abstract static class Builder {

    Builder() {}

    public abstract Builder setPartitionCount(Integer partitionCount);

    public abstract PartitionCountRequest build();
  }
}
