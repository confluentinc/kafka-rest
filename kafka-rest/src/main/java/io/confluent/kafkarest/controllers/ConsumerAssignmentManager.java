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

import io.confluent.kafkarest.entities.ConsumerAssignment;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * A service to manage Kafka {@link ConsumerAssignment Consumer Assignments}.
 */
public interface ConsumerAssignmentManager {

  /**
   * Returns the list of Kafka {@link ConsumerAssignment Consumer Asignments} belonging to the
   * {@link io.confluent.kafkarest.entities.Consumer} with the given {@code consumerId}.
   */
  CompletableFuture<List<ConsumerAssignment>> listConsumerAssignments(
      String clusterId, String consumerGroupId, String consumerId);

  /**
   * Returns the Kafka {@link ConsumerAssignment} with the given {@code topicName} and {@code
   * partitionId}.
   */
  CompletableFuture<Optional<ConsumerAssignment>> getConsumerAssignment(
      String clusterId,
      String consumerGroupId,
      String consumerId,
      String topicName,
      int partitionId
  );
}
