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

import io.confluent.kafkarest.controllers.TopicManager;
import io.confluent.kafkarest.entities.v3.ListReplicasResponse;
import io.confluent.kafkarest.entities.v3.ReplicaDataList;
import io.confluent.kafkarest.entities.v3.ResourceCollection;
import io.confluent.kafkarest.extension.ResourceAccesslistFeature.ResourceName;
import io.confluent.kafkarest.resources.AsyncResponses;
import io.confluent.kafkarest.response.CrnFactory;
import io.confluent.kafkarest.response.UrlFactory;
import io.confluent.rest.annotations.PerformanceMetric;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.MediaType;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

@Path("/v3/clusters/{clusterId}/topics/-/replicas")
@ResourceName("api.v3.topic-configs.*")
public final class ListAllReplicasAction {

  private final Provider<TopicManager> topicManager;
  private final CrnFactory crnFactory;
  private final UrlFactory urlFactory;

  @Inject
  public ListAllReplicasAction(
      Provider<TopicManager> topicManager, CrnFactory crnFactory, UrlFactory urlFactory) {
    this.topicManager = requireNonNull(topicManager);
    this.crnFactory = requireNonNull(crnFactory);
    this.urlFactory = requireNonNull(urlFactory);
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @PerformanceMetric("v3.replicas.list")
  @ResourceName("api.v3.replicas.list")
  public void listReplicas(
      @Suspended AsyncResponse asyncResponse,
      @PathParam("clusterId") String clusterId
  ) {
    CompletableFuture<ListReplicasResponse> response =
        topicManager.get().listTopics(clusterId)
            .thenApply(
                topics ->
                    ListReplicasResponse.create(
                        ReplicaDataList.builder()
                            .setMetadata(
                                ResourceCollection.Metadata.builder()
                                    .setSelf(
                                        urlFactory.create(
                                            "v3",
                                            "clusters",
                                            clusterId,
                                            "replicas"))
                                    .build())
                            .setData(

                                topics.stream().flatMap(
                                    topic -> topic.getPartitions().stream().flatMap(
                                        partition -> partition.getReplicas().stream()
                                    ).map(
                                        partitionReplica ->
                                            ReplicasResource.toReplicaData(
                                                partitionReplica,
                                                crnFactory,
                                                urlFactory
                                            ))
                                        .collect(Collectors.toList()).stream()
                                ).collect(Collectors.toList())
                            ).build()));
    AsyncResponses.asyncResume(asyncResponse, response);
  }

}
