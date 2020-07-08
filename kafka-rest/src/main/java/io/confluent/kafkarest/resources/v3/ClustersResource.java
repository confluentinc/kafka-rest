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

import io.confluent.kafkarest.controllers.ClusterManager;
import io.confluent.kafkarest.entities.Cluster;
import io.confluent.kafkarest.entities.v3.ClusterData;
import io.confluent.kafkarest.entities.v3.ClusterDataList;
import io.confluent.kafkarest.entities.v3.GetClusterResponse;
import io.confluent.kafkarest.entities.v3.ListClustersResponse;
import io.confluent.kafkarest.entities.v3.Resource;
import io.confluent.kafkarest.entities.v3.ResourceCollection;
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
import javax.ws.rs.core.MediaType;

@Path("/v3/clusters")
public final class ClustersResource {

  private final Provider<ClusterManager> clusterManager;
  private final CrnFactory crnFactory;
  private final UrlFactory urlFactory;

  @Inject
  public ClustersResource(
      Provider<ClusterManager> clusterManager,
      CrnFactory crnFactory,
      UrlFactory urlFactory
  ) {
    this.clusterManager = requireNonNull(clusterManager);
    this.crnFactory = requireNonNull(crnFactory);
    this.urlFactory = requireNonNull(urlFactory);
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public void listClusters(@Suspended AsyncResponse asyncResponse) {
    CompletableFuture<ListClustersResponse> response =
        clusterManager.get()
            .listClusters()
            .thenApply(
                clusters ->
                    ListClustersResponse.create(
                        ClusterDataList.builder()
                            .setMetadata(
                                ResourceCollection.Metadata.builder()
                                    .setSelf(urlFactory.create("v3", "clusters"))
                                    .build())
                            .setData(
                                clusters.stream()
                                    .map(this::toClusterData)
                                    .collect(Collectors.toList()))
                            .build()));

    AsyncResponses.asyncResume(asyncResponse, response);
  }

  @GET
  @Path("/{clusterId}")
  @Produces(MediaType.APPLICATION_JSON)
  public void getCluster(
      @Suspended AsyncResponse asyncResponse, @PathParam("clusterId") String clusterId) {
    CompletableFuture<GetClusterResponse> response =
        clusterManager.get()
            .getCluster(clusterId)
            .thenApply(cluster -> cluster.orElseThrow(NotFoundException::new))
            .thenApply(cluster -> GetClusterResponse.create(toClusterData(cluster)));

    AsyncResponses.asyncResume(asyncResponse, response);
  }

  private ClusterData toClusterData(Cluster cluster) {
    ClusterData.Builder clusterData =
        ClusterData.fromCluster(cluster)
            .setMetadata(
                Resource.Metadata.builder()
                    .setSelf(urlFactory.create("v3", "clusters", cluster.getClusterId()))
                    .setResourceName(crnFactory.create("kafka", cluster.getClusterId()))
                    .build())
            .setAcls(
                Resource.Relationship.create(
                    urlFactory.create("v3", "clusters", cluster.getClusterId(), "acls")))
            .setBrokers(
                Resource.Relationship.create(
                    urlFactory.create("v3", "clusters", cluster.getClusterId(), "brokers")))
            .setBrokerConfigs(
                Resource.Relationship.create(
                    urlFactory.create("v3", "clusters", cluster.getClusterId(), "broker-configs")))
            .setConsumerGroups(
                Resource.Relationship.create(
                    urlFactory.create("v3", "clusters", cluster.getClusterId(), "consumer-groups")))
            .setTopics(
                Resource.Relationship.create(
                    urlFactory.create("v3", "clusters", cluster.getClusterId(), "topics")))
            .setPartitionReassignments(
                Resource.Relationship.create(
                    urlFactory.create("v3", "clusters", cluster.getClusterId(), "topics", "-",
                        "partitions", "-", "reassignment")));

    if (cluster.getController() != null) {
      clusterData.setController(
          Resource.Relationship.create(
              urlFactory.create(
                  "v3",
                  "clusters",
                  cluster.getClusterId(),
                  "brokers",
                  Integer.toString(cluster.getController().getBrokerId()))));
    }

    return clusterData.build();
  }
}
