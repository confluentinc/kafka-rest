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

package io.confluent.kafkarest.controllers;

import io.confluent.kafkarest.entities.AlterConfigCommand;
import io.confluent.kafkarest.entities.TopicConfig;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/** A service to manage Kafka {@link TopicConfig TopicConfigs}. */
public interface TopicConfigManager {

  /**
   * Returns the list of Kafka {@link TopicConfig TopicConfigs} belonging to the {@link
   * io.confluent.kafkarest.entities.Topic Topic} with the given {@code topicName}.
   */
  CompletableFuture<List<TopicConfig>> listTopicConfigs(String clusterId, String topicName);

  /**
   * Returns a map of topic name -> Kafka {@link TopicConfig TopicConfigs} belonging to the {@link
   * io.confluent.kafkarest.entities.Topic Topics} with the given {@code topicNames}.
   */
  CompletableFuture<Map<String, List<TopicConfig>>> listTopicConfigs(
      String clusterId, List<String> topicNames);

  /** Returns the Kafka {@link TopicConfig} with the given {@code name}. */
  CompletableFuture<Optional<TopicConfig>> getTopicConfig(
      String clusterId, String topicName, String name);

  /**
   * Updates the Kafka {@link TopicConfig} with the given {@code name} to the given {@code
   * newValue}.
   */
  CompletableFuture<Void> updateTopicConfig(
      String clusterId, String topicName, String name, String newValue);

  /** Resets the Kafka {@link TopicConfig} with the given {@code name} to its default value. */
  CompletableFuture<Void> resetTopicConfig(String clusterId, String topicName, String name);

  /**
   * Atomically alters the Kafka {@link TopicConfig Topic Configs} according to {@code commands}.
   */
  CompletableFuture<Void> alterTopicConfigs(
      String clusterId, String topicName, List<AlterConfigCommand> commands);

  /**
   * Atomically alter configs according to {@code commands}, checking if the configs exist first. If
   * the {@code validateOnly} flag is set, the operation is only dry-ran (the configs do not get
   * altered as a result).
   */
  // KREST-8518 A separate overload is provided instead of changing the pre-existing createTopic
  // method in order to minimize any risks related to external usage of that method (as TopicManager
  // can be injected in projects inheriting from kafka-rest) and to minimize the amount of necessary
  // changes (e.g. by avoiding the need to heavily refactor tests).
  CompletableFuture<Void> alterTopicConfigs(
      String clusterId, String topicName, List<AlterConfigCommand> commands, boolean validateOnly);
}
