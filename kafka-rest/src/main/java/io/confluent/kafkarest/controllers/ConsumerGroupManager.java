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

import io.confluent.kafkarest.entities.ConsumerGroup;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * A service to manage Kafka {@link ConsumerGroup Consumer Groups}.
 */
public interface ConsumerGroupManager {

  /**
   * Returns the list of Kafka {@link ConsumerGroup Consumer Groups} belonging to the {@link
   * io.confluent.kafkarest.entities.Cluster} with the given {@code clusterId}.
   */
  CompletableFuture<List<ConsumerGroup>> listConsumerGroups(String clusterId);

  /**
   * Returns the Kafka {@link ConsumerGroup} with the given {@code consumerGroupId}.
   */
  CompletableFuture<Optional<ConsumerGroup>> getConsumerGroup(
      String clusterId, String consumerGroupId);
}
