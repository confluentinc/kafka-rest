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
import io.confluent.kafkarest.resources.AsyncResponses;
import io.confluent.kafkarest.resources.AsyncResponses.AsyncResponseBuilder;
import io.confluent.kafkarest.response.CrnFactory;
import io.confluent.kafkarest.response.UrlFactory;
import java.util.Comparator;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Provider;
import javax.validation.Valid;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

@Path("/v3/clusters/{clusterId}/{config_type}-configs")
public final class ClusterConfigsResource {

  private final Provider<ClusterConfigManager> clusterConfigManager;
  private final CrnFactory crnFactory;
  private final UrlFactory urlFactory;

  @Inject
  public ClusterConfigsResource(
      Provider<ClusterConfigManager> clusterConfigManager,
      CrnFactory crnFactory,
      UrlFactory urlFactory
  ) {
    this.clusterConfigManager = requireNonNull(clusterConfigManager);
    this.crnFactory = requireNonNull(crnFactory);
    this.urlFactory = requireNonNull(urlFactory);
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public void listClusterConfigs(
      @Suspended AsyncResponse asyncResponse,
      @PathParam("clusterId") String clusterId,
      @PathParam("config_type") ClusterConfig.Type configType
  ) {
    CompletableFuture<ListClusterConfigsResponse> response =
        clusterConfigManager.get().listClusterConfigs(clusterId, configType)
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
  public void getClusterConfig(
      @Suspended AsyncResponse asyncResponse,
      @PathParam("clusterId") String clusterId,
      @PathParam("config_type") ClusterConfig.Type configType,
      @PathParam("name") String name
  ) {
    CompletableFuture<GetClusterConfigResponse> response =
        clusterConfigManager.get()
            .getClusterConfig(clusterId, configType, name)
            .thenApply(config -> config.orElseThrow(NotFoundException::new))
            .thenApply(config -> GetClusterConfigResponse.create(toClusterConfigData(config)));

    AsyncResponses.asyncResume(asyncResponse, response);
  }

  @PUT
  @Path("/{name}")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public void upsertClusterConfig(
      @Suspended AsyncResponse asyncResponse,
      @PathParam("clusterId") String clusterId,
      @PathParam("config_type") ClusterConfig.Type configType,
      @PathParam("name") String name,
      @Valid UpdateClusterConfigRequest request
  ) {
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
  public void deleteClusterConfig(
      @Suspended AsyncResponse asyncResponse,
      @PathParam("clusterId") String clusterId,
      @PathParam("config_type") ClusterConfig.Type configType,
      @PathParam("name") String name
  ) {
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
