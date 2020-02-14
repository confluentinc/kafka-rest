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

public final class Topic {

  private String name;

  private Properties configs;

  private List<Partition> partitions;

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
}
