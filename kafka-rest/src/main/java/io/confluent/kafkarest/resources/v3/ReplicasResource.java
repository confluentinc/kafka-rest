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

import io.confluent.kafkarest.controllers.ReplicaManager;
import io.confluent.kafkarest.entities.PartitionReplica;
import io.confluent.kafkarest.entities.v3.GetReplicaResponse;
import io.confluent.kafkarest.entities.v3.ListReplicasResponse;
import io.confluent.kafkarest.entities.v3.ReplicaData;
import io.confluent.kafkarest.entities.v3.ReplicaDataList;
import io.confluent.kafkarest.entities.v3.Resource;
import io.confluent.kafkarest.entities.v3.ResourceCollection;
import io.confluent.kafkarest.extension.ResourceAccesslistFeature.ResourceName;
import io.confluent.kafkarest.resources.AsyncResponses;
import io.confluent.kafkarest.response.CrnFactory;
import io.confluent.kafkarest.response.UrlFactory;
import io.confluent.rest.annotations.PerformanceMetric;
import jakarta.inject.Inject;
import jakarta.inject.Provider;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.NotFoundException;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.container.AsyncResponse;
import jakarta.ws.rs.container.Suspended;
import jakarta.ws.rs.core.MediaType;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

@Path("/v3/clusters/{clusterId}/topics/{topicName}/partitions/{partitionId}/replicas")
@ResourceName("api.v3.replicas.*")
public final class ReplicasResource {

  private final Provider<ReplicaManager> replicaManager;
  private final CrnFactory crnFactory;
  private final UrlFactory urlFactory;

  @Inject
  public ReplicasResource(
      Provider<ReplicaManager> replicaManager, CrnFactory crnFactory, UrlFactory urlFactory) {
    this.replicaManager = requireNonNull(replicaManager);
    this.crnFactory = requireNonNull(crnFactory);
    this.urlFactory = requireNonNull(urlFactory);
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @PerformanceMetric("v3.replicas.list")
  @ResourceName("api.v3.replicas.list")
  public void listReplicas(
      @Suspended AsyncResponse asyncResponse,
      @PathParam("clusterId") String clusterId,
      @PathParam("topicName") String topicName,
      @PathParam("partitionId") Integer partitionId) {
    CompletableFuture<ListReplicasResponse> response =
        replicaManager
            .get()
            .listReplicas(clusterId, topicName, partitionId)
            .thenApply(
                replicas ->
                    ListReplicasResponse.create(
                        ReplicaDataList.builder()
                            .setMetadata(
                                ResourceCollection.Metadata.builder()
                                    .setSelf(
                                        urlFactory.create(
                                            "v3",
                                            "clusters",
                                            clusterId,
                                            "topics",
                                            topicName,
                                            "partitions",
                                            Integer.toString(partitionId),
                                            "replicas"))
                                    .build())
                            .setData(
                                replicas.stream()
                                    .map(this::toReplicaData)
                                    .collect(Collectors.toList()))
                            .build()));

    AsyncResponses.asyncResume(asyncResponse, response);
  }

  @GET
  @Path("/{brokerId}")
  @Produces(MediaType.APPLICATION_JSON)
  @PerformanceMetric("v3.replicas.get")
  @ResourceName("api.v3.replicas.get")
  public void getReplica(
      @Suspended AsyncResponse asyncResponse,
      @PathParam("clusterId") String clusterId,
      @PathParam("topicName") String topicName,
      @PathParam("partitionId") Integer partitionId,
      @PathParam("brokerId") Integer brokerId) {
    CompletableFuture<GetReplicaResponse> response =
        replicaManager
            .get()
            .getReplica(clusterId, topicName, partitionId, brokerId)
            .thenApply(replica -> replica.orElseThrow(NotFoundException::new))
            .thenApply(replica -> GetReplicaResponse.create(toReplicaData(replica)));

    AsyncResponses.asyncResume(asyncResponse, response);
  }

  private ReplicaData toReplicaData(PartitionReplica replica) {
    return ReplicaData.fromPartitionReplica(replica)
        .setMetadata(
            Resource.Metadata.builder()
                .setSelf(
                    urlFactory.create(
                        "v3",
                        "clusters",
                        replica.getClusterId(),
                        "topics",
                        replica.getTopicName(),
                        "partitions",
                        Integer.toString(replica.getPartitionId()),
                        "replicas",
                        Integer.toString(replica.getBrokerId())))
                .setResourceName(
                    crnFactory.create(
                        "kafka",
                        replica.getClusterId(),
                        "topic",
                        replica.getTopicName(),
                        "partition",
                        Integer.toString(replica.getPartitionId()),
                        "replica",
                        Integer.toString(replica.getBrokerId())))
                .build())
        .setBroker(
            Resource.Relationship.create(
                urlFactory.create(
                    "v3",
                    "clusters",
                    replica.getClusterId(),
                    "brokers",
                    Integer.toString(replica.getBrokerId()))))
        .build();
  }
}
