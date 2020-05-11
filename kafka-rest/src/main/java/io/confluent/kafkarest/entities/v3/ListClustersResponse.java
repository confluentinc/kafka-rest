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
 * Response body for {@code GET /clusters} requests.
 */
public final class ListClustersResponse {

  private final CollectionLink links;

  private final List<ClusterData> data;

  @JsonCreator
  public ListClustersResponse(@JsonProperty("links") CollectionLink links,
                              @JsonProperty("data") List<ClusterData> data) {
    this.links = Objects.requireNonNull(links);
    this.data = Objects.requireNonNull(data);
  }

  @JsonProperty("links")
  public CollectionLink getLinks() {
    return links;
  }

  @JsonProperty("data")
  public List<ClusterData> getData() {
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
    ListClustersResponse that = (ListClustersResponse) o;
    return Objects.equals(links, that.links) && Objects.equals(data, that.data);
  }

  @Override
  public int hashCode() {
    return Objects.hash(links, data);
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", ListClustersResponse.class.getSimpleName() + "[", "]")
        .add("links=" + links)
        .add("data=" + data)
        .toString();
  }
}
