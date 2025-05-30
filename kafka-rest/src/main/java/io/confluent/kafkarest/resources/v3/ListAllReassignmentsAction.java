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

import io.confluent.kafkarest.controllers.ReassignmentManager;
import io.confluent.kafkarest.entities.Reassignment;
import io.confluent.kafkarest.entities.v3.ListAllReassignmentsResponse;
import io.confluent.kafkarest.entities.v3.ReassignmentData;
import io.confluent.kafkarest.entities.v3.ReassignmentDataList;
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
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.container.AsyncResponse;
import jakarta.ws.rs.container.Suspended;
import jakarta.ws.rs.core.MediaType;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

@Path("/v3/clusters/{clusterId}/topics/-/partitions/-/reassignment")
@ResourceName("api.v3.partition-reassignments.*")
public final class ListAllReassignmentsAction {

  private final Provider<ReassignmentManager> reassignmentManager;
  private final CrnFactory crnFactory;
  private final UrlFactory urlFactory;

  @Inject
  public ListAllReassignmentsAction(
      Provider<ReassignmentManager> reassignmentManager,
      CrnFactory crnFactory,
      UrlFactory urlFactory) {
    this.reassignmentManager = requireNonNull(reassignmentManager);
    this.crnFactory = requireNonNull(crnFactory);
    this.urlFactory = requireNonNull(urlFactory);
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @PerformanceMetric("v3.topics-partitions-reassignment.list")
  @ResourceName("api.v3.partition-reassignments.list")
  public void listReassignments(
      @Suspended AsyncResponse asyncResponse, @PathParam("clusterId") String clusterId) {
    CompletableFuture<ListAllReassignmentsResponse> response =
        reassignmentManager
            .get()
            .listReassignments(clusterId)
            .thenApply(
                reassignments ->
                    ListAllReassignmentsResponse.create(
                        ReassignmentDataList.builder()
                            .setMetadata(
                                ResourceCollection.Metadata.builder()
                                    .setSelf(
                                        urlFactory.create(
                                            "v3",
                                            "clusters",
                                            clusterId,
                                            "topics",
                                            "-",
                                            "partitions",
                                            "-",
                                            "reassignments"))
                                    .build())
                            .setData(
                                reassignments.stream()
                                    .map(this::toReassignmentData)
                                    .collect(Collectors.toList()))
                            .build()));

    AsyncResponses.asyncResume(asyncResponse, response);
  }

  private ReassignmentData toReassignmentData(Reassignment reassignment) {
    return ReassignmentData.fromReassignment(reassignment)
        .setMetadata(
            Resource.Metadata.builder()
                .setSelf(
                    urlFactory.create(
                        "v3",
                        "clusters",
                        reassignment.getClusterId(),
                        "topics",
                        reassignment.getTopicName(),
                        "partitions",
                        Integer.toString(reassignment.getPartitionId()),
                        "reassignments"))
                .setResourceName(
                    crnFactory.create(
                        "kafka",
                        reassignment.getClusterId(),
                        "topic",
                        reassignment.getTopicName(),
                        "partition",
                        Integer.toString(reassignment.getPartitionId()),
                        "reassignments",
                        null))
                .build())
        .setReplicas(
            Resource.Relationship.create(
                urlFactory.create(
                    "v3",
                    "clusters",
                    reassignment.getClusterId(),
                    "topics",
                    reassignment.getTopicName(),
                    "partitions",
                    Integer.toString(reassignment.getPartitionId()),
                    "replicas")))
        .build();
  }
}
