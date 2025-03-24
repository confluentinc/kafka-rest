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

import static java.util.Collections.emptySet;

import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableList;
import jakarta.annotation.Nullable;
import java.util.List;
import java.util.Set;

@AutoValue
public abstract class Topic {

  Topic() {}

  public abstract String getClusterId();

  public abstract String getName();

  public abstract ImmutableList<Partition> getPartitions();

  public abstract short getReplicationFactor();

  public abstract boolean isInternal();

  public abstract Set<Acl.Operation> getAuthorizedOperations();

  public static Builder builder() {
    return new AutoValue_Topic.Builder();
  }

  public static Topic create(
      String clusterId,
      String name,
      List<Partition> partitions,
      short replicationFactor,
      boolean isInternal) {
    return create(clusterId, name, partitions, replicationFactor, isInternal, null);
  }

  public static Topic create(
      String clusterId,
      String name,
      List<Partition> partitions,
      short replicationFactor,
      boolean isInternal,
      @Nullable Set<Acl.Operation> authorizedOperations) {
    return builder()
        .setClusterId(clusterId)
        .setName(name)
        .addAllPartitions(partitions)
        .setReplicationFactor(replicationFactor)
        .setInternal(isInternal)
        .setAuthorizedOperations(authorizedOperations == null ? emptySet() : authorizedOperations)
        .build();
  }

  public Builder toBuilder() {
    return builder()
        .setClusterId(getClusterId())
        .setName(getName())
        .addAllPartitions(getPartitions())
        .setReplicationFactor(getReplicationFactor())
        .setInternal(isInternal())
        .setAuthorizedOperations(getAuthorizedOperations());
  }

  @AutoValue.Builder
  public abstract static class Builder {

    Builder() {}

    public abstract Builder setClusterId(String clusterId);

    public abstract Builder setName(String name);

    abstract ImmutableList.Builder<Partition> partitionsBuilder();

    public final Builder addAllPartitions(Iterable<Partition> partitions) {
      partitionsBuilder().addAll(partitions);
      return this;
    }

    public abstract Builder setReplicationFactor(short replicationFactor);

    public abstract Builder setInternal(boolean isInternal);

    public abstract Builder setAuthorizedOperations(Set<Acl.Operation> authorizedOperations);

    public abstract Topic build();
  }
}
