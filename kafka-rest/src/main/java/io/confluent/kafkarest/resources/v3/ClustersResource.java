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

import io.confluent.kafkarest.Versions;
import io.confluent.kafkarest.controllers.ClusterManager;
import io.confluent.kafkarest.entities.Cluster;
import io.confluent.kafkarest.entities.v3.ClusterData;
import io.confluent.kafkarest.entities.v3.CollectionLink;
import io.confluent.kafkarest.entities.v3.GetClusterResponse;
import io.confluent.kafkarest.entities.v3.ListClustersResponse;
import io.confluent.kafkarest.entities.v3.Relationship;
import io.confluent.kafkarest.entities.v3.ResourceLink;
import io.confluent.kafkarest.resources.AsyncResponses;
import io.confluent.kafkarest.response.CrnFactory;
import io.confluent.kafkarest.response.UrlFactory;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Provider;
import javax.ws.rs.GET;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;

@Path("/v3/clusters")
public final class ClustersResource {

  private final Provider<ClusterManager> clusterManager;
  private final CrnFactory crnFactory;
  private final UrlFactory urlFactory;

  @Inject
  public ClustersResource(
      Provider<ClusterManager> clusterManager, CrnFactory crnFactory, UrlFactory urlFactory) {
    this.clusterManager = requireNonNull(clusterManager);
    this.crnFactory = requireNonNull(crnFactory);
    this.urlFactory = requireNonNull(urlFactory);
  }

  @GET
  @Produces(Versions.JSON_API)
  public void listClusters(@Suspended AsyncResponse asyncResponse) {
    CompletableFuture<ListClustersResponse> response =
        clusterManager.get()
            .listClusters()
            .thenApply(
                clusters ->
                    new ListClustersResponse(
                        new CollectionLink(urlFactory.create("v3", "clusters"), /* next= */ null),
                        clusters.stream().map(this::toClusterData).collect(Collectors.toList())));

    AsyncResponses.asyncResume(asyncResponse, response);
  }

  @GET
  @Path("/{clusterId}")
  @Produces(Versions.JSON_API)
  public void getCluster(
      @Suspended AsyncResponse asyncResponse, @PathParam("clusterId") String clusterId) {
    CompletableFuture<GetClusterResponse> response =
        clusterManager.get()
            .getCluster(clusterId)
            .thenApply(cluster -> cluster.orElseThrow(NotFoundException::new))
            .thenApply(cluster -> new GetClusterResponse(toClusterData(cluster)));

    AsyncResponses.asyncResume(asyncResponse, response);
  }

  private ClusterData toClusterData(Cluster cluster) {
    Relationship controller;
    if (cluster.getController() != null) {
      controller =
          new Relationship(
              urlFactory.create(
                  "v3",
                  "clusters",
                  cluster.getClusterId(),
                  "brokers",
                  Integer.toString(cluster.getController().getBrokerId())));
    } else {
      controller = null;
    }

    Relationship brokers =
        new Relationship(urlFactory.create("v3", "clusters", cluster.getClusterId(), "brokers"));
    Relationship topics =
        new Relationship(urlFactory.create("v3", "clusters", cluster.getClusterId(), "topics"));

    return new ClusterData(
        crnFactory.create(ClusterData.ELEMENT_TYPE, cluster.getClusterId()),
        new ResourceLink(urlFactory.create("v3", "clusters", cluster.getClusterId())),
        cluster.getClusterId(),
        controller,
        brokers,
        topics);
  }
}
