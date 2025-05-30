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

import io.confluent.kafkarest.controllers.ConsumerAssignmentManager;
import io.confluent.kafkarest.entities.ConsumerAssignment;
import io.confluent.kafkarest.entities.v3.ConsumerAssignmentData;
import io.confluent.kafkarest.entities.v3.ConsumerAssignmentDataList;
import io.confluent.kafkarest.entities.v3.GetConsumerAssignmentResponse;
import io.confluent.kafkarest.entities.v3.ListConsumerAssignmentsResponse;
import io.confluent.kafkarest.entities.v3.Resource;
import io.confluent.kafkarest.entities.v3.Resource.Relationship;
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
import java.util.Comparator;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

@Path(
    "/v3/clusters/{clusterId}/consumer-groups/{consumerGroupId}/consumers/{consumerId}/assignments")
@ResourceName("api.v3.consumer-assignments.*")
public final class ConsumerAssignmentsResource {

  private final Provider<ConsumerAssignmentManager> consumerAssignmentManager;
  private final CrnFactory crnFactory;
  private final UrlFactory urlFactory;

  @Inject
  public ConsumerAssignmentsResource(
      Provider<ConsumerAssignmentManager> consumerAssignmentManager,
      CrnFactory crnFactory,
      UrlFactory urlFactory) {
    this.consumerAssignmentManager = requireNonNull(consumerAssignmentManager);
    this.crnFactory = requireNonNull(crnFactory);
    this.urlFactory = requireNonNull(urlFactory);
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @PerformanceMetric("v3.consumer-assignments.list")
  @ResourceName("api.v3.consumer-assignments.list")
  public void listConsumerAssignments(
      @Suspended AsyncResponse asyncResponse,
      @PathParam("clusterId") String clusterId,
      @PathParam("consumerGroupId") String consumerGroupId,
      @PathParam("consumerId") String consumerId) {
    CompletableFuture<ListConsumerAssignmentsResponse> response =
        consumerAssignmentManager
            .get()
            .listConsumerAssignments(clusterId, consumerGroupId, consumerId)
            .thenApply(
                assignments ->
                    ListConsumerAssignmentsResponse.create(
                        ConsumerAssignmentDataList.builder()
                            .setMetadata(
                                ResourceCollection.Metadata.builder()
                                    .setSelf(
                                        urlFactory.create(
                                            "v3",
                                            "clusters",
                                            clusterId,
                                            "consumer-groups",
                                            consumerGroupId,
                                            "consumers",
                                            consumerId,
                                            "assignments"))
                                    .build())
                            .setData(
                                assignments.stream()
                                    .map(this::toConsumerAssignmentData)
                                    .sorted(
                                        Comparator.comparing(ConsumerAssignmentData::getTopicName)
                                            .thenComparing(ConsumerAssignmentData::getPartitionId))
                                    .collect(Collectors.toList()))
                            .build()));

    AsyncResponses.asyncResume(asyncResponse, response);
  }

  @GET
  @Path("/{topicName}/partitions/{partitionId}")
  @Produces(MediaType.APPLICATION_JSON)
  @PerformanceMetric("v3.consumer-assignments.get")
  @ResourceName("api.v3.consumer-assignments.get")
  public void getConsumerAssignment(
      @Suspended AsyncResponse asyncResponse,
      @PathParam("clusterId") String clusterId,
      @PathParam("consumerGroupId") String consumerGroupId,
      @PathParam("consumerId") String consumerId,
      @PathParam("topicName") String topicName,
      @PathParam("partitionId") int partitionId) {
    CompletableFuture<GetConsumerAssignmentResponse> response =
        consumerAssignmentManager
            .get()
            .getConsumerAssignment(clusterId, consumerGroupId, consumerId, topicName, partitionId)
            .thenApply(assignment -> assignment.orElseThrow(NotFoundException::new))
            .thenApply(
                assignment ->
                    GetConsumerAssignmentResponse.create(toConsumerAssignmentData(assignment)));

    AsyncResponses.asyncResume(asyncResponse, response);
  }

  private ConsumerAssignmentData toConsumerAssignmentData(ConsumerAssignment assignment) {
    return ConsumerAssignmentData.fromConsumerAssignment(assignment)
        .setMetadata(
            Resource.Metadata.builder()
                .setSelf(
                    urlFactory.create(
                        "v3",
                        "clusters",
                        assignment.getClusterId(),
                        "consumer-groups",
                        assignment.getConsumerGroupId(),
                        "consumers",
                        assignment.getConsumerId(),
                        "assignments",
                        assignment.getTopicName(),
                        "partitions",
                        Integer.toString(assignment.getPartitionId())))
                .setResourceName(
                    crnFactory.create(
                        "kafka",
                        assignment.getClusterId(),
                        "consumer-group",
                        assignment.getConsumerGroupId(),
                        "consumer",
                        assignment.getConsumerId(),
                        "assignment",
                        assignment.getTopicName(),
                        "partition",
                        Integer.toString(assignment.getPartitionId())))
                .build())
        .setPartition(
            Relationship.create(
                urlFactory.create(
                    "v3",
                    "clusters",
                    assignment.getClusterId(),
                    "topics",
                    assignment.getTopicName(),
                    "partitions",
                    Integer.toString(assignment.getPartitionId()))))
        .setLag(
            Relationship.create(
                urlFactory.create(
                    "v3",
                    "clusters",
                    assignment.getClusterId(),
                    "consumer-groups",
                    assignment.getConsumerGroupId(),
                    "lags",
                    assignment.getTopicName(),
                    "partitions",
                    Integer.toString(assignment.getPartitionId()))))
        .build();
  }
}
