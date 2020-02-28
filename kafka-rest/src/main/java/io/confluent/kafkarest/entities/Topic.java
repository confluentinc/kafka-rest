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

import java.util.*;

public final class Topic {

  private final String name;

  private final Properties configs;

  private final List<Partition> partitions;

  public Topic(String name, Properties configs, List<Partition> partitions) {
    if (name.isEmpty()) {
      throw new IllegalArgumentException();
    }
    if (partitions.isEmpty()) {
      throw new IllegalArgumentException();
    }
    this.name = name;
    this.configs = Objects.requireNonNull(configs);
    this.partitions = partitions;
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
        && Objects.equals(partitions, topic.partitions);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, configs, partitions);
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", Topic.class.getSimpleName() + "[", "]")
        .add("name='" + name + "'")
        .add("configs=" + configs)
        .add("partitions=" + partitions)
        .toString();
  }

  public static final class Builder {
    private String topicName;
    private Properties configs;
    private List<Partition> partitions = new ArrayList<>();

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

    public Topic build() {
      return new Topic(topicName, configs, partitions);
    }
  }
}
