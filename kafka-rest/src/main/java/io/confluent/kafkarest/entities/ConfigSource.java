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

import org.apache.kafka.clients.admin.ConfigEntry;

/**
 * Source of a {@link AbstractConfig config} value.
 *
 * @see org.apache.kafka.clients.admin.ConfigEntry.ConfigSource
 * @see <a href=
 *      https://cwiki.apache.org/confluence/display/KAFKA/KIP-226+-+Dynamic+Broker+Configuration">
 *      KIP-226 - Dynamic Broker Configuration</a>
 */
public enum ConfigSource {

  /**
   * Dynamic cluster link config that is configured for a specific link name.
   */
  DYNAMIC_CLUSTER_LINK_CONFIG,

  /**
   * Dynamic topic config that is configured for a specific topic.
   */
  DYNAMIC_TOPIC_CONFIG,

  /**
   * Dynamic broker logger config that is configured for a specific broker.
   */
  DYNAMIC_BROKER_LOGGER_CONFIG,

  /**
   * Dynamic broker config that is configured for a specific broker.
   */
  DYNAMIC_BROKER_CONFIG,

  /**
   * Dynamic broker config that is configured as default for all brokers in the cluster.
   */
  DYNAMIC_DEFAULT_BROKER_CONFIG,

  /**
   * Static broker config provided as broker properties at start up (e.g. server.properties file).
   */
  STATIC_BROKER_CONFIG,

  /**
   * Built-in default configuration for configs that have a default value.
   */
  DEFAULT_CONFIG,

  /**
   * Source unknown e.g. in the ConfigEntry used for alter requests where source is not set.
   */
  UNKNOWN;

  public static ConfigSource fromAdminConfigSource(ConfigEntry.ConfigSource source) {
    try {
      return valueOf(source.name());
    } catch (IllegalArgumentException e) {
      return UNKNOWN;
    }
  }
}
