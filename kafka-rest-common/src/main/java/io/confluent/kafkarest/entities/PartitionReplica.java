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

package io.confluent.kafkarest.entities;

import java.util.Objects;
import java.util.StringJoiner;

public final class PartitionReplica {

  private final int broker;

  private final boolean leader;

  private final boolean inSync;

  public PartitionReplica(int broker, boolean leader, boolean inSync) {
    this.broker = broker;
    this.leader = leader;
    this.inSync = inSync;
  }

  public int getBroker() {
    return broker;
  }

  public boolean isLeader() {
    return leader;
  }

  public boolean isInSync() {
    return inSync;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    PartitionReplica that = (PartitionReplica) o;
    return broker == that.broker && leader == that.leader && inSync == that.inSync;
  }

  @Override
  public int hashCode() {
    return Objects.hash(broker, leader, inSync);
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", PartitionReplica.class.getSimpleName() + "[", "]")
        .add("broker=" + broker)
        .add("leader=" + leader)
        .add("inSync=" + inSync)
        .toString();
  }
}
