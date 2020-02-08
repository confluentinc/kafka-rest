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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.hibernate.validator.constraints.NotEmpty;

import java.util.List;
import java.util.Objects;
import java.util.Properties;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class Topic {

  @NotEmpty
  private String name;

  private Properties configs;

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
    return Objects.equals(name, topic.name)
           && Objects.equals(configs, topic.configs)
           && Objects.equals(partitions, topic.partitions);
  }

  @Override
  public int hashCode() {
    int result = name != null ? name.hashCode() : 0;
    result = 31 * result + (partitions != null ? partitions.hashCode() : 0);
    result = 31 * result + (configs != null ? configs.hashCode() : 0);
    return result;
  }
}
