/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.kafkarest.entities.v2;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.Objects;
import java.util.StringJoiner;
import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;

public final class BrokerList {

  @NotNull
  @Nullable
  private final List<Integer> brokers;

  @JsonCreator
  public BrokerList(@JsonProperty("brokers") @Nullable List<Integer> brokers) {
    this.brokers = brokers;
  }

  @JsonProperty
  @Nullable
  public List<Integer> getBrokers() {
    return brokers;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    BrokerList that = (BrokerList) o;
    return Objects.equals(brokers, that.brokers);
  }

  @Override
  public int hashCode() {
    return Objects.hash(brokers);
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", BrokerList.class.getSimpleName() + "[", "]")
        .add("brokers=" + brokers)
        .toString();
  }
}
