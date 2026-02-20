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

package io.confluent.kafkarest.resources.v3;

import static java.util.Objects.requireNonNull;

import io.confluent.kafkarest.controllers.BrokerConfigManager;
import io.confluent.kafkarest.entities.BrokerConfig;
import io.confluent.kafkarest.entities.v3.BrokerConfigData;
import io.confluent.kafkarest.entities.v3.BrokerConfigDataList;
import io.confluent.kafkarest.entities.v3.GetBrokerConfigResponse;
import io.confluent.kafkarest.entities.v3.ListBrokerConfigsResponse;
import io.confluent.kafkarest.entities.v3.Resource;
import io.confluent.kafkarest.entities.v3.ResourceCollection;
import io.confluent.kafkarest.entities.v3.UpdateBrokerConfigRequest;
import io.confluent.kafkarest.extension.ResourceAccesslistFeature.ResourceName;
import io.confluent.kafkarest.resources.AsyncResponses;
import io.confluent.kafkarest.resources.AsyncResponses.AsyncResponseBuilder;
import io.confluent.kafkarest.response.CrnFactory;
import io.confluent.kafkarest.response.UrlFactory;
import io.confluent.rest.annotations.PerformanceMetric;
import jakarta.inject.Inject;
import jakarta.inject.Provider;
import jakarta.validation.Valid;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.NotFoundException;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.container.AsyncResponse;
import jakarta.ws.rs.container.Suspended;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.Response.Status;
import java.util.Comparator;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

@Path("/v3/clusters/{clusterId}/brokers/{brokerId}/configs")
@ResourceName("api.v3.broker-configs.*")
public final class BrokerConfigsResource {

  private final Provider<BrokerConfigManager> brokerConfigManager;
  private final CrnFactory crnFactory;
  private final UrlFactory urlFactory;

  @Inject
  public BrokerConfigsResource(
      Provider<BrokerConfigManager> brokerConfigManager,
      CrnFactory crnFactory,
      UrlFactory urlFactory) {
    this.brokerConfigManager = requireNonNull(brokerConfigManager);
    this.crnFactory = requireNonNull(crnFactory);
    this.urlFactory = requireNonNull(urlFactory);
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @PerformanceMetric("v3.brokers.configs.list")
  @ResourceName("api.v3.broker-configs.list")
  public void listBrokerConfigs(
      @Suspended AsyncResponse asyncResponse,
      @PathParam("clusterId") String clusterId,
      @PathParam("brokerId") int brokerId) {
    CompletableFuture<ListBrokerConfigsResponse> response =
        brokerConfigManager
            .get()
            .listBrokerConfigs(clusterId, brokerId)
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
                                            String.valueOf(brokerId),
                                            "configs"))
                                    .build())
                            .setData(
                                configs.stream()
                                    .sorted(Comparator.comparing(BrokerConfig::getName))
                                    .map(
                                        brokerConfig ->
                                            toBrokerConfigData(
                                                brokerConfig, crnFactory, urlFactory))
                                    .collect(Collectors.toList()))
                            .build()));

    AsyncResponses.asyncResume(asyncResponse, response);
  }

  @GET
  @Path("/{name}")
  @Produces(MediaType.APPLICATION_JSON)
  @PerformanceMetric("v3.brokers.configs.get")
  @ResourceName("api.v3.broker-configs.get")
  public void getBrokerConfig(
      @Suspended AsyncResponse asyncResponse,
      @PathParam("clusterId") String clusterId,
      @PathParam("brokerId") int brokerId,
      @PathParam("name") String name) {
    CompletableFuture<GetBrokerConfigResponse> response =
        brokerConfigManager
            .get()
            .getBrokerConfig(clusterId, brokerId, name)
            .thenApply(broker -> broker.orElseThrow(NotFoundException::new))
            .thenApply(
                broker ->
                    GetBrokerConfigResponse.create(
                        toBrokerConfigData(broker, crnFactory, urlFactory)));

    AsyncResponses.asyncResume(asyncResponse, response);
  }

  @PUT
  @Path("/{name}")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @PerformanceMetric("v3.brokers.configs.update")
  @ResourceName("api.v3.broker-configs.update")
  public void updateBrokerConfig(
      @Suspended AsyncResponse asyncResponse,
      @PathParam("clusterId") String clusterId,
      @PathParam("brokerId") int brokerId,
      @PathParam("name") String name,
      @Valid UpdateBrokerConfigRequest request) {
    String newValue = request.getValue().orElse(null);

    CompletableFuture<Void> response =
        brokerConfigManager.get().updateBrokerConfig(clusterId, brokerId, name, newValue);

    AsyncResponseBuilder.from(Response.status(Status.NO_CONTENT))
        .entity(response)
        .asyncResume(asyncResponse);
  }

  @DELETE
  @Path("/{name}")
  @Produces(MediaType.APPLICATION_JSON)
  @PerformanceMetric("v3.brokers.configs.delete")
  @ResourceName("api.v3.broker-configs.delete")
  public void resetBrokerConfig(
      @Suspended AsyncResponse asyncResponse,
      @PathParam("clusterId") String clusterId,
      @PathParam("brokerId") int brokerId,
      @PathParam("name") String name) {
    CompletableFuture<Void> response =
        brokerConfigManager.get().resetBrokerConfig(clusterId, brokerId, name);

    AsyncResponseBuilder.from(Response.status(Status.NO_CONTENT))
        .entity(response)
        .asyncResume(asyncResponse);
  }

  public static BrokerConfigData toBrokerConfigData(
      BrokerConfig brokerConfig, CrnFactory crnFactory, UrlFactory urlFactory) {
    return BrokerConfigData.fromBrokerConfig(brokerConfig)
        .setMetadata(
            Resource.Metadata.builder()
                .setSelf(
                    urlFactory.create(
                        "v3",
                        "clusters",
                        brokerConfig.getClusterId(),
                        "brokers",
                        String.valueOf(brokerConfig.getBrokerId()),
                        "configs",
                        brokerConfig.getName()))
                .setResourceName(
                    crnFactory.create(
                        "kafka",
                        brokerConfig.getClusterId(),
                        "broker",
                        String.valueOf(brokerConfig.getBrokerId()),
                        "config",
                        brokerConfig.getName()))
                .build())
        .build();
  }
}
