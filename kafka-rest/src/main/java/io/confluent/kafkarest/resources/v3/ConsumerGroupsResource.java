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

import io.confluent.kafkarest.controllers.ConsumerGroupManager;
import io.confluent.kafkarest.entities.ConsumerGroup;
import io.confluent.kafkarest.entities.v3.ConsumerGroupData;
import io.confluent.kafkarest.entities.v3.ConsumerGroupDataList;
import io.confluent.kafkarest.entities.v3.GetConsumerGroupResponse;
import io.confluent.kafkarest.entities.v3.ListConsumerGroupsResponse;
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
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

@Path("/v3/clusters/{clusterId}/consumer-groups")
@ResourceName("api.v3.consumer-groups.*")
public final class ConsumerGroupsResource {

  private final Provider<ConsumerGroupManager> consumerGroupManager;
  private final CrnFactory crnFactory;
  private final UrlFactory urlFactory;

  @Inject
  public ConsumerGroupsResource(
      Provider<ConsumerGroupManager> consumerGroupManager,
      CrnFactory crnFactory,
      UrlFactory urlFactory) {
    this.consumerGroupManager = requireNonNull(consumerGroupManager);
    this.crnFactory = requireNonNull(crnFactory);
    this.urlFactory = requireNonNull(urlFactory);
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @PerformanceMetric("v3.consumer-groups.list")
  @ResourceName("api.v3.consumer-groups.list")
  public void listConsumerGroups(
      @Suspended AsyncResponse asyncResponse, @PathParam("clusterId") String clusterId) {
    CompletableFuture<ListConsumerGroupsResponse> response =
        consumerGroupManager
            .get()
            .listConsumerGroups(clusterId)
            .thenApply(
                consumerGroups ->
                    ListConsumerGroupsResponse.create(
                        ConsumerGroupDataList.builder()
                            .setMetadata(
                                ResourceCollection.Metadata.builder()
                                    .setSelf(
                                        urlFactory.create(
                                            "v3", "clusters", clusterId, "consumer-groups"))
                                    .build())
                            .setData(
                                consumerGroups.stream()
                                    .map(
                                        consumerGroup ->
                                            toConsumerGroupData(clusterId, consumerGroup))
                                    .collect(Collectors.toList()))
                            .build()));

    AsyncResponses.asyncResume(asyncResponse, response);
  }

  @GET
  @Path("/{consumerGroupId}")
  @Produces(MediaType.APPLICATION_JSON)
  @PerformanceMetric("v3.consumer-groups.get")
  @ResourceName("api.v3.consumer-groups.get")
  public void getConsumerGroup(
      @Suspended AsyncResponse asyncResponse,
      @PathParam("clusterId") String clusterId,
      @PathParam("consumerGroupId") String consumerGroupId) {
    CompletableFuture<GetConsumerGroupResponse> response =
        consumerGroupManager
            .get()
            .getConsumerGroup(clusterId, consumerGroupId)
            .thenApply(consumerGroup -> consumerGroup.orElseThrow(NotFoundException::new))
            .thenApply(
                consumerGroup ->
                    GetConsumerGroupResponse.create(toConsumerGroupData(clusterId, consumerGroup)));

    AsyncResponses.asyncResume(asyncResponse, response);
  }

  private ConsumerGroupData toConsumerGroupData(String clusterId, ConsumerGroup consumerGroup) {
    return ConsumerGroupData.fromConsumerGroup(consumerGroup)
        .setMetadata(
            Resource.Metadata.builder()
                .setSelf(
                    urlFactory.create(
                        "v3",
                        "clusters",
                        clusterId,
                        "consumer-groups",
                        consumerGroup.getConsumerGroupId()))
                .setResourceName(
                    crnFactory.create(
                        "kafka", clusterId, "consumer-group", consumerGroup.getConsumerGroupId()))
                .build())
        .setCoordinator(
            Relationship.create(
                urlFactory.create(
                    "v3",
                    "clusters",
                    clusterId,
                    "brokers",
                    Integer.toString(consumerGroup.getCoordinator().getBrokerId()))))
        .setConsumers(
            Relationship.create(
                urlFactory.create(
                    "v3",
                    "clusters",
                    clusterId,
                    "consumer-groups",
                    consumerGroup.getConsumerGroupId(),
                    "consumers")))
        .setLagSummary(
            Relationship.create(
                urlFactory.create(
                    "v3",
                    "clusters",
                    clusterId,
                    "consumer-groups",
                    consumerGroup.getConsumerGroupId(),
                    "lag-summary")))
        .build();
  }
}
