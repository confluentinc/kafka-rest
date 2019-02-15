/*
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

import org.hibernate.validator.constraints.NotEmpty;

import java.util.List;
import java.util.Properties;

import javax.validation.constraints.NotNull;

public class Topic {

  @NotEmpty
  private String name;

  @NotNull
  private Properties configs;

  @NotEmpty
  private List<Partition> partitions;

  public Topic(
      @JsonProperty("name") String name,
      @JsonProperty("configs") Properties configs,
      @JsonProperty("partitions") List<Partition> partitions
  ) {
    this.name = name;
    this.configs = configs;
    this.partitions = partitions;
  }

  @JsonProperty
  public String getName() {
    return name;
  }

  @JsonProperty
  public void setName(String name) {
    this.name = name;
  }

  @JsonProperty
  public Properties getConfigs() {
    return configs;
  }

  @JsonProperty
  public void setConfigs(Properties configs) {
    this.configs = configs;
  }

  @JsonProperty
  public List<Partition> getPartitions() {
    return partitions;
  }

  @JsonProperty
  public void setPartitions(List<Partition> partitions) {
    this.partitions = partitions;
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

    if (configs != null ? !configs.equals(topic.configs) : topic.configs != null) {
      return false;
    }
    if (name != null ? !name.equals(topic.name) : topic.name != null) {
      return false;
    }
    if (partitions != null ? !partitions.equals(topic.partitions) : topic.partitions != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int result = name != null ? name.hashCode() : 0;
    result = 31 * result + (partitions != null ? partitions.hashCode() : 0);
    result = 31 * result + (configs != null ? configs.hashCode() : 0);
    return result;
  }
}
