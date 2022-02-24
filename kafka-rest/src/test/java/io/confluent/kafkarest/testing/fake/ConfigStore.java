/*
 * Copyright 2022 Confluent Inc.
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

package io.confluent.kafkarest.testing.fake;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableMap;
import java.util.function.Function;
import java.util.stream.Stream;
import org.apache.kafka.common.config.ConfigResource;

final class ConfigStore {

  private static final ConfigDef<Short> DEFAULT_REPLICATION_FACTOR =
      ConfigDef.shortConfig(
          ConfigResource.Type.BROKER,
          "default.replication.factor",
          (short) 1,
          (value) -> value > 0);
  private static final ConfigDef<Integer> NUM_PARTITIONS =
      ConfigDef.intConfig(ConfigResource.Type.BROKER, "num.partitions", 1, (value) -> value > 0);

  private static final ImmutableMap<String, ConfigDef<?>> CONFIG_DEFS_BY_NAME =
      Stream.of(DEFAULT_REPLICATION_FACTOR, NUM_PARTITIONS)
          .collect(toImmutableMap(ConfigDef::getName, Function.identity()));

  private final ConfigResource.Type resource;

  ConfigStore(ConfigResource.Type resource) {
    this.resource = requireNonNull(resource);
  }

  /** Returns the default replication factor for automatically created topics. */
  short getDefaultReplicationFactor() {
    return DEFAULT_REPLICATION_FACTOR.parse(getConfig(DEFAULT_REPLICATION_FACTOR.getName()));
  }

  /** Returns the default number of log partitions per topic. */
  int getNumPartitions() {
    return NUM_PARTITIONS.parse(getConfig(NUM_PARTITIONS.getName()));
  }

  /** Returns the current value for the config with the given {@code name}. */
  private String getConfig(String name) {
    ConfigDef<?> configDef = CONFIG_DEFS_BY_NAME.get(name);
    checkArgument(
        configDef != null && configDef.getResource().equals(resource),
        "Config `%s' doesn't exist.",
        name);
    return configDef.getDefaultValue();
  }
}
