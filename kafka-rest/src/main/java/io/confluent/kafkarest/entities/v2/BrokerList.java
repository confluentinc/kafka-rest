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
import javax.validation.constraints.NotNull;

public class BrokerList {

  @NotNull
  private List<Integer> brokers;

  @JsonCreator
  public BrokerList(@JsonProperty("brokers") List<Integer> brokers) {
    this.brokers = brokers;
  }

  @JsonProperty
  public List<Integer> getBrokers() {
    return brokers;
  }

  @JsonProperty
  public void setBrokers(List<Integer> brokers) {
    this.brokers = brokers;
  }

  @Override
  public String toString() {
    return "BrokerList{" + "brokers=" + brokers + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof BrokerList)) {
      return false;
    }

    BrokerList that = (BrokerList) o;

    return brokers.equals(that.brokers);
  }

  @Override
  public int hashCode() {
    return brokers.hashCode();
  }
}
