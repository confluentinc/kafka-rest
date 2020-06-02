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
import java.util.Objects;
import java.util.StringJoiner;

/**
 * The {@code links} property of a resource type.
 */
public final class ResourceLink {

  private final String self;

  @JsonCreator
  public ResourceLink(@JsonProperty("self") String self) {
    this.self = Objects.requireNonNull(self);
  }

  @JsonProperty("self")
  public String getSelf() {
    return self;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ResourceLink that = (ResourceLink) o;
    return Objects.equals(self, that.self);
  }

  @Override
  public int hashCode() {
    return Objects.hash(self);
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", ResourceLink.class.getSimpleName() + "[", "]")
        .add("self='" + self + "'")
        .toString();
  }
}
