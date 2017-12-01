/**
 * Copyright 2015 Confluent Inc.
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
 **/

package io.confluent.kafkarest.entities;

import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.constraints.Min;

public class PartitionReplica {

  @Min(0)
  private int broker;
  private boolean leader;
  private boolean inSync;

  public PartitionReplica() {
  }

  public PartitionReplica(int broker, boolean leader, boolean inSync) {
    this.broker = broker;
    this.leader = leader;
    this.inSync = inSync;
  }

  @JsonProperty
  public int getBroker() {
    return broker;
  }

  @JsonProperty
  public void setBroker(int broker) {
    this.broker = broker;
  }

  @JsonProperty
  public boolean isLeader() {
    return leader;
  }

  @JsonProperty
  public void setLeader(boolean leader) {
    this.leader = leader;
  }

  @JsonProperty("in_sync")
  public boolean isInSync() {
    return inSync;
  }

  @JsonProperty("in_sync")
  public void setInSync(boolean inSync) {
    this.inSync = inSync;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof PartitionReplica)) {
      return false;
    }

    PartitionReplica that = (PartitionReplica) o;

    if (broker != that.broker) {
      return false;
    }
    if (inSync != that.inSync) {
      return false;
    }
    if (leader != that.leader) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int result = broker;
    result = 31 * result + (leader ? 1 : 0);
    result = 31 * result + (inSync ? 1 : 0);
    return result;
  }

  @Override
  public String toString() {
    return "PartitionReplica{"
           + "broker=" + broker
           + ", leader=" + leader
           + ", inSync=" + inSync
           + '}';
  }
}
