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
import java.util.List;
import java.util.Objects;
import java.util.StringJoiner;

/**
 * Response body for {@code GET
 * /v3/clusters/<clusterId>/topics/<topicName>/partitions/<partitionId>/replicas} requests.
 */
public final class ListReplicasResponse {

  private final CollectionLink links;

  private final List<ReplicaData> data;

  @JsonCreator
  public ListReplicasResponse(@JsonProperty("links") CollectionLink links,
                              @JsonProperty("data") List<ReplicaData> data) {
    this.links = Objects.requireNonNull(links);
    this.data = Objects.requireNonNull(data);
  }

  @JsonProperty("links")
  public CollectionLink getLinks() {
    return links;
  }

  @JsonProperty("data")
  public List<ReplicaData> getData() {
    return data;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ListReplicasResponse that = (ListReplicasResponse) o;
    return Objects.equals(links, that.links) && Objects.equals(data, that.data);
  }

  @Override
  public int hashCode() {
    return Objects.hash(links, data);
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", ListReplicasResponse.class.getSimpleName() + "[", "]")
        .add("links=" + links)
        .add("data=" + data)
        .toString();
  }
}
