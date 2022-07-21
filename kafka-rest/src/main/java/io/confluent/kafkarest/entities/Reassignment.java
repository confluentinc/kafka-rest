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

package io.confluent.kafkarest.entities;

import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableList;
import java.util.List;

@AutoValue
public abstract class Reassignment {

  Reassignment() {
  }

  public abstract String getClusterId();

  public abstract String getTopicName();

  public abstract int getPartitionId();

  public abstract ImmutableList<Integer> getAddingReplicas();

  public abstract ImmutableList<Integer> getRemovingReplicas();

  public static Reassignment create(
      String clusterId,
      String topicName,
      int partitionId,
      List<Integer> addingReplicas,
      List<Integer> removingReplicas) {
    return new AutoValue_Reassignment(
        clusterId,
        topicName,
        partitionId,
        ImmutableList.copyOf(addingReplicas),
        ImmutableList.copyOf(removingReplicas)
    );
  }
}
