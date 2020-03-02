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


import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.StringJoiner;

public final class Topic {

  private final String name;

  private final Properties configs;

  private final List<Partition> partitions;

  private final long replicationFactor;

  private final boolean isInternal;

  public Topic(String name,
               Properties configs,
               List<Partition> partitions,
               long replicationFactor,
               boolean isInternal) {
    if (name.isEmpty()) {
      throw new IllegalArgumentException();
    }
    if (partitions.isEmpty()) {
      throw new IllegalArgumentException();
    }
    this.name = name;
    this.configs = Objects.requireNonNull(configs);
    this.partitions = partitions;
    this.replicationFactor = replicationFactor;
    this.isInternal = isInternal;
  }

  public Topic(String name,
               Properties configs,
               List<Partition> partitions) {
    this(name, configs, partitions, 0, false);
  }

  public String getName() {
    return name;
  }

  public Properties getConfigs() {
    return configs;
  }

  public List<Partition> getPartitions() {
    return partitions;
  }

  public long getReplicationFactor() {
    return replicationFactor;
  }

  public boolean getIsInternal() {
    return isInternal;
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
    return Objects.equals(name, topic.name)
        && Objects.equals(configs, topic.configs)
        && Objects.equals(partitions, topic.partitions)
        && Objects.equals(replicationFactor, topic.replicationFactor)
        && Objects.equals(isInternal, topic.isInternal);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, configs, partitions, isInternal, replicationFactor);
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", Topic.class.getSimpleName() + "[", "]")
        .add("name='" + name + "'")
        .add("configs=" + configs)
        .add("partitions=" + partitions)
        .add("replication factor=" + replicationFactor)
        .add("isInternal="+isInternal)
        .toString();
  }

  public static final class Builder {
    private String topicName;
    private Properties configs;
    private List<Partition> partitions = new ArrayList<>();
    private long replicationFactor;
    private boolean isInternal;

    public Builder() {
    }

    public Builder setTopicName(String topicName) {
      this.topicName = topicName;
      return this;
    }

    public Builder setConfigs(Properties configs) {
      this.configs = configs;
      return this;
    }

    public Builder setPartitions(List<Partition> partitions) {
      this.partitions.addAll(partitions);
      return this;
    }

    public Builder setReplicationFactor(long replicationFactor) {
      this.replicationFactor = replicationFactor;
      return this;
    }

    public Builder setIsInternal(boolean isInternal) {
      this.isInternal = isInternal;
      return this;
    }

    public Topic build() {
      return new Topic(topicName, configs, partitions, replicationFactor, isInternal);
    }
  }
}
