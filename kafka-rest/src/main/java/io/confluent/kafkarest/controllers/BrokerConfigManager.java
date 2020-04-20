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

import io.confluent.kafkarest.entities.BrokerConfig;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public interface BrokerConfigManager {

  /**
   * Returns a list of Kafka {@link BrokerConfig BrokerConfigs} belonging  to the
   * {@link io.confluent.kafkarest.entities.Broker} with the given {@code brokerId}.
   */
  CompletableFuture<List<BrokerConfig>> listBrokerConfigs(String clusterId, String brokerId);

  /**
   * Returns the Kafka {@link BrokerConfig} with the given {@code name}.
   */
  CompletableFuture<Optional<BrokerConfig>> getBrokerConfig(
      String clusterId, String brokerId, String name);

}
