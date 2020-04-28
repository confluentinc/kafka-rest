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

import io.confluent.kafkarest.entities.Topic;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * A service to manage Kafka {@link Topic Topics}.
 */
public interface TopicManager {

  /**
   * Returns the list of Kafka {@link Topic Topics} belonging to the {@link
   * io.confluent.kafkarest.entities.Cluster} with the given {@code clusterId}.
   */
  CompletableFuture<List<Topic>> listTopics(String clusterId);

  /**
   * Returns the list of Kafka {@link Topic Topics} belonging to the {@link
   * io.confluent.kafkarest.entities.Cluster} that this application is connected to.
   */
  CompletableFuture<List<Topic>> listLocalTopics();

  /**
   * Returns the Kafka {@link Topic} with the given {@code topicName}.
   */
  CompletableFuture<Optional<Topic>> getTopic(String clusterId, String topicName);

  /**
   * Returns the Kafka {@link Topic} with the given {@code topicName}, belonging to the {@link
   * io.confluent.kafkarest.entities.Cluster} that this application is connected to.
   */
  CompletableFuture<Optional<Topic>> getLocalTopic(String topicName);

  /**
   * Creates a new Kafka {@link Topic}.
   */
  CompletableFuture<Void> createTopic(
      String clusterId,
      String topicName,
      int partitionsCount,
      short replicationFactor,
      Map<String, String> configs);

  /**
   * Deletes the Kafka {@link Topic} with the given {@code topicName}.
   */
  CompletableFuture<Void> deleteTopic(String clusterId, String topicName);
}
