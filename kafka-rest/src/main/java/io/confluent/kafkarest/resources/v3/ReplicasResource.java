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
import io.confluent.kafkarest.controllers.ReplicaManager;
import io.confluent.kafkarest.entities.v3.CollectionLink;
import io.confluent.kafkarest.entities.v3.GetReplicaResponse;
import io.confluent.kafkarest.entities.v3.ListReplicasResponse;
import io.confluent.kafkarest.entities.v3.ReplicaData;
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

@Path("/v3/clusters/{clusterId}/topics/{topicName}/partitions/{partitionId}/replicas")
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
  @Produces(Versions.JSON_API)
  public void listReplicas(
      @Suspended AsyncResponse asyncResponse,
      @PathParam("clusterId") String clusterId,
      @PathParam("topicName") String topicName,
      @PathParam("partitionId") Integer partitionId
  ) {
    CompletableFuture<ListReplicasResponse> response =
        replicaManager.get()
            .listReplicas(clusterId, topicName, partitionId)
            .thenApply(
                replicas ->
                    new ListReplicasResponse(
                        new CollectionLink(
                            urlFactory.create(
                                "v3",
                                "clusters",
                                clusterId,
                                "topics",
                                topicName,
                                "partitions",
                                Integer.toString(partitionId),
                                "replicas"),
                            /* next= */ null),
                        replicas.stream()
                            .map(replica -> ReplicaData.create(crnFactory, urlFactory, replica))
                            .collect(Collectors.toList())));

    AsyncResponses.asyncResume(asyncResponse, response);
  }

  @GET
  @Path("/{brokerId}")
  @Produces(Versions.JSON_API)
  public void getReplica(
      @Suspended AsyncResponse asyncResponse,
      @PathParam("clusterId") String clusterId,
      @PathParam("topicName") String topicName,
      @PathParam("partitionId") Integer partitionId,
      @PathParam("brokerId") Integer brokerId
  ) {
    CompletableFuture<GetReplicaResponse> response =
        replicaManager.get()
            .getReplica(clusterId, topicName, partitionId, brokerId)
            .thenApply(replica -> replica.orElseThrow(NotFoundException::new))
            .thenApply(replica ->
                new GetReplicaResponse(ReplicaData.create(crnFactory, urlFactory, replica)));

    AsyncResponses.asyncResume(asyncResponse, response);
  }
}
