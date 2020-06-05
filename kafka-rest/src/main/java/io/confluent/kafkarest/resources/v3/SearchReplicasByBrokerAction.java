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

import static java.util.Comparator.comparing;
import static java.util.Objects.requireNonNull;

import io.confluent.kafkarest.controllers.ReplicaManager;
import io.confluent.kafkarest.entities.PartitionReplica;
import io.confluent.kafkarest.entities.v3.ReplicaData;
import io.confluent.kafkarest.entities.v3.ReplicaDataList;
import io.confluent.kafkarest.entities.v3.Resource;
import io.confluent.kafkarest.entities.v3.ResourceCollection;
import io.confluent.kafkarest.entities.v3.SearchReplicasByBrokerResponse;
import io.confluent.kafkarest.resources.AsyncResponses;
import io.confluent.kafkarest.response.CrnFactory;
import io.confluent.kafkarest.response.UrlFactory;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Provider;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.MediaType;

@Path("/v3/clusters/{clusterId}/brokers/{brokerId}/partition-replicas")
public final class SearchReplicasByBrokerAction {

  private final Provider<ReplicaManager> replicaManager;
  private final CrnFactory crnFactory;
  private final UrlFactory urlFactory;

  @Inject
  public SearchReplicasByBrokerAction(
      Provider<ReplicaManager> replicaManager, CrnFactory crnFactory, UrlFactory urlFactory) {
    this.replicaManager = requireNonNull(replicaManager);
    this.crnFactory = requireNonNull(crnFactory);
    this.urlFactory = requireNonNull(urlFactory);
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public void searchReplicasByBroker(
      @Suspended AsyncResponse asyncResponse,
      @PathParam("clusterId") String clusterId,
      @PathParam("brokerId") Integer brokerId
  ) {
    CompletableFuture<SearchReplicasByBrokerResponse> response =
        replicaManager.get()
            .searchReplicasByBrokerId(clusterId, brokerId)
            .thenApply(
                replicas ->
                    SearchReplicasByBrokerResponse.create(
                        ReplicaDataList.builder()
                            .setMetadata(
                                ResourceCollection.Metadata.builder()
                                    .setSelf(
                                        urlFactory.create(
                                            "v3",
                                            "clusters",
                                            clusterId,
                                            "brokers",
                                            Integer.toString(brokerId),
                                            "partition-replicas"))
                                    .build())
                            .setData(
                                replicas.stream()
                                    .sorted(
                                        comparing(PartitionReplica::getTopicName)
                                            .thenComparing(PartitionReplica::getPartitionId))
                                    .map(this::toReplicaData)
                                    .collect(Collectors.toList()))
                            .build()));

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
