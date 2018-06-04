/*
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.kafkarest.entities;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collections;
import java.util.List;

/**
 * Contains broker information
 */
public class BrokerEntity {

  public static BrokerEntity empty() {
    return new BrokerEntity(-1, Collections.<EndPointEntity>emptyList());
  }

  private Integer brokerId;
  private List<EndPointEntity> endPointList;

  @JsonCreator
  public BrokerEntity(@JsonProperty("brokerId") Integer brokerId,
                      @JsonProperty("endPointList") List<EndPointEntity> endPointList) {
    this.brokerId = brokerId;
    this.endPointList = endPointList;
  }

  @JsonProperty
  public Integer getBrokerId() {
    return brokerId;
  }

  @JsonProperty
  public void setBrokerId(Integer brokerId) {
    this.brokerId = brokerId;
  }

  @JsonProperty
  public List<EndPointEntity> getEndPointList() {
    return endPointList;
  }

  @JsonProperty
  public void setEndPointList(List<EndPointEntity> endPointList) {
    this.endPointList = endPointList;
  }

  @Override
  public String toString() {
    return "BrokerEntity{"
            + "brokerId=" + brokerId
            + ", endPointList=" + endPointList
            + '}';
  }



  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    BrokerEntity that = (BrokerEntity) o;

    return brokerId.equals(that.brokerId) && endPointList.equals(that.endPointList);
  }

  @Override
  public int hashCode() {
    int result = brokerId.hashCode();
    result = 31 * result + endPointList.hashCode();
    return result;
  }
}
