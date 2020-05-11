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
 * Response body for {@code GET /v3/clusters/<clusterId>/brokers/<brokerId>/configs/<name>}
 * requests.
 */
public final class GetBrokerConfigResponse {

  private final BrokerConfigData data;

  @JsonCreator
  public GetBrokerConfigResponse(@JsonProperty("data") BrokerConfigData data) {
    this.data = Objects.requireNonNull(data);
  }

  @JsonProperty
  public BrokerConfigData getData() {
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
    GetBrokerConfigResponse that = (GetBrokerConfigResponse) o;
    return Objects.equals(data, that.data);
  }

  @Override
  public int hashCode() {
    return Objects.hash(data);
  }

  @Override
  public String toString() {
    return new StringJoiner(
        ", ", GetBrokerConfigResponse.class.getSimpleName() + "[", "]")
        .add("data=" + data)
        .toString();
  }
}
