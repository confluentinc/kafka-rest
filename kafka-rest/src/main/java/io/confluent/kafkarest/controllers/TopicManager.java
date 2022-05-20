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

/** A service to manage Kafka {@link Topic Topics}. */
public interface TopicManager {

  /**
   * Returns the list of Kafka {@link Topic Topics} belonging to the {@link
   * io.confluent.kafkarest.entities.Cluster} with the given {@code clusterId}.
   */
  default CompletableFuture<List<Topic>> listTopics(String clusterId) {
    return listTopics(clusterId, false);
  }

  /**
   * Returns the list of Kafka {@link Topic Topics} belonging to the {@link
   * io.confluent.kafkarest.entities.Cluster} with the given {@code clusterId} and a flag to
   * determine whether to return authorised operations: {@code includeAuthorizedOperations}.
   */
  CompletableFuture<List<Topic>> listTopics(String clusterId, boolean includeAuthorizedOperations);

  /**
   * Returns the list of Kafka {@link Topic Topics} belonging to the {@link
   * io.confluent.kafkarest.entities.Cluster} that this application is connected to.
   */
  CompletableFuture<List<Topic>> listLocalTopics();

  /** Returns the Kafka {@link Topic} with the given {@code topicName}. */
  default CompletableFuture<Optional<Topic>> getTopic(String clusterId, String topicName) {
    return getTopic(clusterId, topicName, false);
  }

  /**
   * Returns the Kafka {@link Topic} with the given {@code topicName} and a flag to determine
   * whether to return authorised operations: {@code includeAuthorizedOperations}.
   */
  CompletableFuture<Optional<Topic>> getTopic(
      String clusterId, String topicName, boolean includeAuthorizedOperations);

  /**
   * Returns the Kafka {@link Topic} with the given {@code topicName}, belonging to the {@link
   * io.confluent.kafkarest.entities.Cluster} that this application is connected to.
   */
  CompletableFuture<Optional<Topic>> getLocalTopic(String topicName);

  /**
   * Creates a new Kafka {@link Topic} with either partitions count and replication factor or
   * explicitly specified partition-to-replicas assignments.
   */
  CompletableFuture<Void> createTopic(
      String clusterId,
      String topicName,
      Optional<Integer> partitionsCount,
      Optional<Short> replicationFactor,
      Map<Integer, List<Integer>> replicasAssignments,
      Map<String, Optional<String>> configs);

  /** Deletes the Kafka {@link Topic} with the given {@code topicName}. */
  CompletableFuture<Void> deleteTopic(String clusterId, String topicName);
}
