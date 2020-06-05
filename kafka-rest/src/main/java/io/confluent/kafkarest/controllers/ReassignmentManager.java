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

import io.confluent.kafkarest.entities.Reassignment;
import io.confluent.kafkarest.entities.Topic;
import io.confluent.kafkarest.entities.Partition;
import io.confluent.kafkarest.entities.Cluster;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public interface ReassignmentManager {

  /**
   * Returns a list of Kafka {@link Reassignment Reassignments} for all Kafka {@link Topic Topics}
   * for a Kafka {@link Cluster} with the given {@code clusterId}.
   */
  CompletableFuture<List<Reassignment>> listReassignments(String clusterId);

  /**
   * Returns a list of Kafka {@link Reassignment Reassignments} for Kafka {@link Topic Topic} with
   * the given {@code topicName} for a Kafka {@link Cluster} with the given {@code clusterId}.
   */
  CompletableFuture<List<Reassignment>> searchReassignmentsByTopicName(
      String clusterId, String topicName);

  /**
   * Returns a list of Kafka {@link Reassignment Reassignments} for a Kafka {@link Topic Topic} with
   * given {@code topicName} for a Kafka {@link Partition Partition} with the given {@code
   * partitionId} for a Kafka {@link Cluster Cluster} with the given {@code clusterId}.
   */
  CompletableFuture<Optional<Reassignment>> getReassignment(
      String clusterId, String topicName, Integer partitionId);
}
