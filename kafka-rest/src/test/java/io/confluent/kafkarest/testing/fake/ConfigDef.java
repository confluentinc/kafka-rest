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

import static java.util.Objects.requireNonNull;

import java.util.function.Function;
import java.util.function.Predicate;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.InvalidConfigurationException;

final class ConfigDef<T> {
  private final ConfigResource.Type resource;
  private final String name;
  private final String defaultValue;
  private final Function<String, T> parser;
  private final Predicate<T> validator;

  private ConfigDef(
      ConfigResource.Type resource,
      String name,
      String defaultValue,
      Function<String, T> parser,
      Predicate<T> validator) {
    this.resource = requireNonNull(resource);
    this.name = requireNonNull(name);
    this.defaultValue = requireNonNull(defaultValue);
    this.parser = requireNonNull(parser);
    this.validator = requireNonNull(validator);
    parse(defaultValue);
  }

  ConfigResource.Type getResource() {
    return resource;
  }

  String getName() {
    return name;
  }

  String getDefaultValue() {
    return defaultValue;
  }

  T parse(String value) {
    T parsed;
    try {
      parsed = parser.apply(value);
    } catch (IllegalArgumentException e) {
      throw new InvalidConfigurationException(
          String.format("`%s' is an invalid value for config `%s'", value, name), e);
    }
    if (!validator.test(parsed)) {
      throw new InvalidConfigurationException(
          String.format("`%s' is an invalid value for config `%s'", value, name));
    }
    return parsed;
  }

  static ConfigDef<Integer> intConfig(
      ConfigResource.Type resource, String name, int defaultValue, Predicate<Integer> validator) {
    return new ConfigDef<>(
        resource, name, Integer.toString(defaultValue), Integer::parseInt, validator);
  }

  static ConfigDef<Short> shortConfig(
      ConfigResource.Type resource, String name, short defaultValue, Predicate<Short> validator) {
    return new ConfigDef<>(
        resource, name, Short.toString(defaultValue), Short::parseShort, validator);
  }
}
