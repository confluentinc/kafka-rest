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


import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.StringJoiner;

public final class Topic {

  private final String clusterId;

  private final String name;

  private final Properties configurations;

  private final List<Partition> partitions;

  private final short replicationFactor;

  private final boolean isInternal;

  public Topic(
      String clusterId,
      String name,
      Properties configurations,
      List<Partition> partitions,
      short replicationFactor,
      boolean isInternal) {
    if (name.isEmpty()) {
      throw new IllegalArgumentException();
    }
    this.clusterId = Objects.requireNonNull(clusterId);
    this.name = name;
    this.configurations = Objects.requireNonNull(configurations);
    this.partitions = Objects.requireNonNull(partitions);
    this.replicationFactor = replicationFactor;
    this.isInternal = isInternal;
  }

  /**
   * @deprecated use {@link #Topic(String, String, Properties, List, short, boolean)} instead
   */
  @Deprecated
  public Topic(String name, Properties configs, List<Partition> partitions) {
    this(
        /* clusterId= */ "",
        name,
        configs,
        partitions,
        /* replicationFactor= */ (short) 0,
        /* isInternal= */ false);
  }

  public String getName() {
    return name;
  }

  public Properties getConfigs() {
    return configurations;
  }

  public List<Partition> getPartitions() {
    return partitions;
  }

  public short getReplicationFactor() {
    return replicationFactor;
  }

  public boolean getIsInternal() {
    return isInternal;
  }

  public String getClusterId() {
    return clusterId;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Topic topic = (Topic) o;
    return replicationFactor == topic.replicationFactor
        && isInternal == topic.isInternal
        && Objects.equals(clusterId, topic.clusterId)
        && Objects.equals(name, topic.name)
        && Objects.equals(configurations, topic.configurations)
        && Objects.equals(partitions, topic.partitions);
  }

  @Override
  public int hashCode() {
    return Objects.hash(clusterId, name, configurations, partitions, replicationFactor, isInternal);
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", Topic.class.getSimpleName() + "[", "]")
        .add("clusterId='" + clusterId + "'")
        .add("name='" + name + "'")
        .add("configurations=" + configurations)
        .add("partitions=" + partitions)
        .add("replicationFactor=" + replicationFactor)
        .add("isInternal=" + isInternal)
        .toString();
  }
}
