/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.kafkarest.resources.v2;

import static java.util.Objects.requireNonNull;

import io.confluent.kafkarest.Versions;
import io.confluent.kafkarest.controllers.BrokerManager;
import io.confluent.kafkarest.entities.Broker;
import io.confluent.kafkarest.entities.v2.BrokerList;
import io.confluent.kafkarest.extension.ResourceAccesslistFeature.ResourceName;
import io.confluent.kafkarest.resources.AsyncResponses;
import io.confluent.rest.annotations.PerformanceMetric;
import jakarta.inject.Inject;
import jakarta.inject.Provider;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.container.AsyncResponse;
import jakarta.ws.rs.container.Suspended;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/** Resource representing the collection of all available brokers. */
@Path("/brokers")
@Produces(Versions.KAFKA_V2_JSON_WEIGHTED)
@Consumes()
@ResourceName("api.v2.brokers.*")
public final class BrokersResource {

  private final Provider<BrokerManager> brokerManager;

  @Inject
  BrokersResource(Provider<BrokerManager> brokerManager) {
    this.brokerManager = requireNonNull(brokerManager);
  }

  @GET
  @PerformanceMetric("brokers.list+v2")
  @ResourceName("api.v2.brokers.list")
  public void list(@Suspended AsyncResponse asyncResponse) {
    CompletableFuture<BrokerList> response =
        brokerManager
            .get()
            .listLocalBrokers()
            .thenApply(
                brokers ->
                    new BrokerList(
                        brokers.stream().map(Broker::getBrokerId).collect(Collectors.toList())));

    AsyncResponses.asyncResume(asyncResponse, response);
  }
}
