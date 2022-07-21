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
import java.util.List;

@AutoValue
public abstract class ClusterDataList extends ResourceCollection<ClusterData> {

  ClusterDataList() {
  }

  public static Builder builder() {
    return new AutoValue_ClusterDataList.Builder().setKind("KafkaClusterList");
  }

  @JsonCreator
  static ClusterDataList fromJson(
      @JsonProperty("kind") String kind,
      @JsonProperty("metadata") Metadata metadata,
      @JsonProperty("data") List<ClusterData> data
  ) {
    return builder()
        .setKind(kind)
        .setMetadata(metadata)
        .setData(data)
        .build();
  }

  @AutoValue.Builder
  public abstract static class Builder
      extends ResourceCollection.Builder<ClusterData, ClusterDataList, Builder> {

    Builder() {
    }
  }
}
