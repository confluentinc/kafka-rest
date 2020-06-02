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
import javax.annotation.Nullable;

/**
 * The {@code links} property of a collection.
 */
public final class CollectionLink {

  private final String self;

  @Nullable
  private final String next;

  @JsonCreator
  public CollectionLink(@JsonProperty("self") String self,
                        @JsonProperty("next") @Nullable String next) {
    this.self = Objects.requireNonNull(self);
    this.next = next;
  }

  @JsonProperty("self")
  public String getSelf() {
    return self;
  }

  @JsonProperty("next")
  @Nullable
  public String getNext() {
    return next;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    CollectionLink that = (CollectionLink) o;
    return Objects.equals(self, that.self) && Objects.equals(next, that.next);
  }

  @Override
  public int hashCode() {
    return Objects.hash(self, next);
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", CollectionLink.class.getSimpleName() + "[", "]")
        .add("self='" + self + "'")
        .add("next='" + next + "'")
        .toString();
  }
}
