/*
 * Copyright 2021 Confluent Inc.
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

package io.confluent.kafkarest.resources.v3;

import static java.util.Objects.requireNonNull;

import io.confluent.kafkarest.controllers.BrokerConfigManager;
import io.confluent.kafkarest.controllers.BrokerManager;
import io.confluent.kafkarest.entities.Broker;
import io.confluent.kafkarest.entities.BrokerConfig;
import io.confluent.kafkarest.entities.v3.BrokerConfigDataList;
import io.confluent.kafkarest.entities.v3.ListBrokerConfigsResponse;
import io.confluent.kafkarest.entities.v3.ResourceCollection;
import io.confluent.kafkarest.extension.ResourceAccesslistFeature.ResourceName;
import io.confluent.kafkarest.resources.AsyncResponses;
import io.confluent.kafkarest.response.CrnFactory;
import io.confluent.kafkarest.response.UrlFactory;
import io.confluent.rest.annotations.PerformanceMetric;
import jakarta.inject.Inject;
import jakarta.inject.Provider;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.container.AsyncResponse;
import jakarta.ws.rs.container.Suspended;
import jakarta.ws.rs.core.MediaType;
import java.util.Comparator;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

@Path("/v3/clusters/{clusterId}/brokers/-/configs")
@ResourceName("api.v3.brokers-configs.*")
public final class ListAllBrokersConfigsAction {

  private final Provider<BrokerManager> brokerManager;
  private final Provider<BrokerConfigManager> brokerConfigManager;
  private final CrnFactory crnFactory;
  private final UrlFactory urlFactory;

  @Inject
  public ListAllBrokersConfigsAction(
      Provider<BrokerManager> brokerManager,
      Provider<BrokerConfigManager> brokerConfigManager,
      CrnFactory crnFactory,
      UrlFactory urlFactory) {
    this.brokerManager = brokerManager;
    this.brokerConfigManager = requireNonNull(brokerConfigManager);
    this.crnFactory = requireNonNull(crnFactory);
    this.urlFactory = requireNonNull(urlFactory);
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @PerformanceMetric("v3.brokers.configs.list")
  @ResourceName("api.v3.brokers-configs.list")
  public void listBrokersConfigs(
      @Suspended AsyncResponse asyncResponse, @PathParam("clusterId") String clusterId) {
    BrokerConfigManager resolvedBrokerConfigManager = brokerConfigManager.get();

    CompletableFuture<ListBrokerConfigsResponse> response =
        brokerManager
            .get()
            .listBrokers(clusterId)
            .thenCompose(
                brokers ->
                    resolvedBrokerConfigManager
                        .listAllBrokerConfigs(
                            clusterId,
                            brokers.stream().map(Broker::getBrokerId).collect(Collectors.toList()))
                        .thenApply(
                            configs ->
                                ListBrokerConfigsResponse.create(
                                    BrokerConfigDataList.builder()
                                        .setMetadata(
                                            ResourceCollection.Metadata.builder()
                                                .setSelf(
                                                    urlFactory.create(
                                                        "v3",
                                                        "clusters",
                                                        clusterId,
                                                        "brokers",
                                                        "-",
                                                        "configs"))
                                                .build())
                                        .setData(
                                            configs.values().stream()
                                                .flatMap(
                                                    brokerConfigs ->
                                                        brokerConfigs.stream()
                                                            .sorted(
                                                                Comparator.comparing(
                                                                    BrokerConfig::getBrokerId)))
                                                .map(
                                                    brokerConfig ->
                                                        BrokerConfigsResource.toBrokerConfigData(
                                                            brokerConfig, crnFactory, urlFactory))
                                                .collect(Collectors.toList()))
                                        .build())));

    AsyncResponses.asyncResume(asyncResponse, response);
  }
}
