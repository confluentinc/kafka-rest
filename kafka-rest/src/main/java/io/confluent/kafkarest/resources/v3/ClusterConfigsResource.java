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

import io.confluent.kafkarest.controllers.ClusterConfigManager;
import io.confluent.kafkarest.entities.ClusterConfig;
import io.confluent.kafkarest.entities.v3.ClusterConfigData;
import io.confluent.kafkarest.entities.v3.ClusterConfigDataList;
import io.confluent.kafkarest.entities.v3.GetClusterConfigResponse;
import io.confluent.kafkarest.entities.v3.ListClusterConfigsResponse;
import io.confluent.kafkarest.entities.v3.Resource;
import io.confluent.kafkarest.entities.v3.ResourceCollection;
import io.confluent.kafkarest.entities.v3.UpdateClusterConfigRequest;
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

@Path("/v3/clusters/{clusterId}/{config_type}-configs")
@ResourceName("api.v3.cluster-configs.*")
public final class ClusterConfigsResource {

  private final Provider<ClusterConfigManager> clusterConfigManager;
  private final CrnFactory crnFactory;
  private final UrlFactory urlFactory;

  @Inject
  public ClusterConfigsResource(
      Provider<ClusterConfigManager> clusterConfigManager,
      CrnFactory crnFactory,
      UrlFactory urlFactory) {
    this.clusterConfigManager = requireNonNull(clusterConfigManager);
    this.crnFactory = requireNonNull(crnFactory);
    this.urlFactory = requireNonNull(urlFactory);
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @PerformanceMetric("v3.clusters.configs.list")
  @ResourceName("api.v3.cluster-configs.list")
  public void listClusterConfigs(
      @Suspended AsyncResponse asyncResponse,
      @PathParam("clusterId") String clusterId,
      @PathParam("config_type") ClusterConfig.Type configType) {
    CompletableFuture<ListClusterConfigsResponse> response =
        clusterConfigManager
            .get()
            .listClusterConfigs(clusterId, configType)
            .thenApply(
                configs ->
                    ListClusterConfigsResponse.create(
                        ClusterConfigDataList.builder()
                            .setMetadata(
                                ResourceCollection.Metadata.builder()
                                    .setSelf(
                                        urlFactory.create(
                                            "v3",
                                            "clusters",
                                            clusterId,
                                            String.format(
                                                "%s-configs", configType.name().toLowerCase())))
                                    .build())
                            .setData(
                                configs.stream()
                                    .sorted(
                                        Comparator.comparing(ClusterConfig::getType)
                                            .thenComparing(ClusterConfig::getName))
                                    .map(this::toClusterConfigData)
                                    .collect(Collectors.toList()))
                            .build()));

    AsyncResponses.asyncResume(asyncResponse, response);
  }

  @GET
  @Path("/{name}")
  @Produces(MediaType.APPLICATION_JSON)
  @PerformanceMetric("v3.clusters.configs.get")
  @ResourceName("api.v3.cluster-configs.get")
  public void getClusterConfig(
      @Suspended AsyncResponse asyncResponse,
      @PathParam("clusterId") String clusterId,
      @PathParam("config_type") ClusterConfig.Type configType,
      @PathParam("name") String name) {
    CompletableFuture<GetClusterConfigResponse> response =
        clusterConfigManager
            .get()
            .getClusterConfig(clusterId, configType, name)
            .thenApply(config -> config.orElseThrow(NotFoundException::new))
            .thenApply(config -> GetClusterConfigResponse.create(toClusterConfigData(config)));

    AsyncResponses.asyncResume(asyncResponse, response);
  }

  @PUT
  @Path("/{name}")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @PerformanceMetric("v3.clusters.configs.update")
  @ResourceName("api.v3.cluster-configs.update")
  public void upsertClusterConfig(
      @Suspended AsyncResponse asyncResponse,
      @PathParam("clusterId") String clusterId,
      @PathParam("config_type") ClusterConfig.Type configType,
      @PathParam("name") String name,
      @Valid UpdateClusterConfigRequest request) {
    String newValue = request.getValue().orElse(null);

    CompletableFuture<Void> response =
        clusterConfigManager.get().upsertClusterConfig(clusterId, configType, name, newValue);

    AsyncResponseBuilder.from(Response.status(Status.NO_CONTENT))
        .entity(response)
        .asyncResume(asyncResponse);
  }

  @DELETE
  @Path("/{name}")
  @Produces(MediaType.APPLICATION_JSON)
  @PerformanceMetric("v3.clusters.configs.delete")
  @ResourceName("api.v3.cluster-configs.delete")
  public void deleteClusterConfig(
      @Suspended AsyncResponse asyncResponse,
      @PathParam("clusterId") String clusterId,
      @PathParam("config_type") ClusterConfig.Type configType,
      @PathParam("name") String name) {
    CompletableFuture<Void> response =
        clusterConfigManager.get().deleteClusterConfig(clusterId, configType, name);

    AsyncResponseBuilder.from(Response.status(Status.NO_CONTENT))
        .entity(response)
        .asyncResume(asyncResponse);
  }

  private ClusterConfigData toClusterConfigData(ClusterConfig clusterConfig) {
    return ClusterConfigData.fromClusterConfig(clusterConfig)
        .setMetadata(
            Resource.Metadata.builder()
                .setSelf(
                    urlFactory.create(
                        "v3",
                        "clusters",
                        clusterConfig.getClusterId(),
                        String.format("%s-configs", clusterConfig.getType().name().toLowerCase()),
                        clusterConfig.getName()))
                .setResourceName(
                    crnFactory.create(
                        "kafka",
                        clusterConfig.getClusterId(),
                        String.format("%s-config", clusterConfig.getType().name().toLowerCase()),
                        clusterConfig.getName()))
                .build())
        .build();
  }
}
