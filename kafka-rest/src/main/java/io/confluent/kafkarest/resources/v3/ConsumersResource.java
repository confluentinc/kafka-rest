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

import io.confluent.kafkarest.controllers.ConsumerManager;
import io.confluent.kafkarest.entities.Consumer;
import io.confluent.kafkarest.entities.v3.ConsumerData;
import io.confluent.kafkarest.entities.v3.ConsumerDataList;
import io.confluent.kafkarest.entities.v3.GetConsumerResponse;
import io.confluent.kafkarest.entities.v3.ListConsumersResponse;
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

@Path("/v3/clusters/{clusterId}/consumer-groups/{consumerGroupId}/consumers")
@ResourceName("api.v3.consumers.*")
public final class ConsumersResource {

  private final Provider<ConsumerManager> consumerManager;
  private final CrnFactory crnFactory;
  private final UrlFactory urlFactory;

  @Inject
  public ConsumersResource(
      Provider<ConsumerManager> consumerManager, CrnFactory crnFactory, UrlFactory urlFactory) {
    this.consumerManager = requireNonNull(consumerManager);
    this.crnFactory = requireNonNull(crnFactory);
    this.urlFactory = requireNonNull(urlFactory);
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @PerformanceMetric("v3.consumers.list")
  @ResourceName("api.v3.consumers.list")
  public void listConsumers(
      @Suspended AsyncResponse asyncResponse,
      @PathParam("clusterId") String clusterId,
      @PathParam("consumerGroupId") String consumerGroupId) {
    CompletableFuture<ListConsumersResponse> response =
        consumerManager
            .get()
            .listConsumers(clusterId, consumerGroupId)
            .thenApply(
                consumers ->
                    ListConsumersResponse.create(
                        ConsumerDataList.builder()
                            .setMetadata(
                                ResourceCollection.Metadata.builder()
                                    .setSelf(
                                        urlFactory.create(
                                            "v3",
                                            "clusters",
                                            clusterId,
                                            "consumer-groups",
                                            consumerGroupId,
                                            "consumers"))
                                    .build())
                            .setData(
                                consumers.stream()
                                    .map(this::toConsumerData)
                                    .sorted(Comparator.comparing(ConsumerData::getConsumerId))
                                    .collect(Collectors.toList()))
                            .build()));

    AsyncResponses.asyncResume(asyncResponse, response);
  }

  @GET
  @Path("/{consumerId}")
  @Produces(MediaType.APPLICATION_JSON)
  @PerformanceMetric("v3.consumers.get")
  @ResourceName("api.v3.consumers.get")
  public void getConsumer(
      @Suspended AsyncResponse asyncResponse,
      @PathParam("clusterId") String clusterId,
      @PathParam("consumerGroupId") String consumerGroupId,
      @PathParam("consumerId") String consumerId) {
    CompletableFuture<GetConsumerResponse> response =
        consumerManager
            .get()
            .getConsumer(clusterId, consumerGroupId, consumerId)
            .thenApply(consumer -> consumer.orElseThrow(NotFoundException::new))
            .thenApply(consumer -> GetConsumerResponse.create(toConsumerData(consumer)));

    AsyncResponses.asyncResume(asyncResponse, response);
  }

  private ConsumerData toConsumerData(Consumer consumer) {
    return ConsumerData.fromConsumer(consumer)
        .setMetadata(
            Resource.Metadata.builder()
                .setSelf(
                    urlFactory.create(
                        "v3",
                        "clusters",
                        consumer.getClusterId(),
                        "consumer-groups",
                        consumer.getConsumerGroupId(),
                        "consumers",
                        consumer.getConsumerId()))
                .setResourceName(
                    crnFactory.create(
                        "kafka",
                        consumer.getClusterId(),
                        "consumer-group",
                        consumer.getConsumerGroupId(),
                        "consumer",
                        consumer.getConsumerId()))
                .build())
        .setAssignments(
            Relationship.create(
                urlFactory.create(
                    "v3",
                    "clusters",
                    consumer.getClusterId(),
                    "consumer-groups",
                    consumer.getConsumerGroupId(),
                    "consumers",
                    consumer.getConsumerId(),
                    "assignments")))
        .build();
  }
}
