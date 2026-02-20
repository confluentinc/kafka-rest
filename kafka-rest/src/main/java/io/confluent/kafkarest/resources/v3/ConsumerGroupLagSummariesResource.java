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

import io.confluent.kafkarest.controllers.ConsumerGroupLagSummaryManager;
import io.confluent.kafkarest.entities.ConsumerGroupLagSummary;
import io.confluent.kafkarest.entities.v3.ConsumerGroupLagSummaryData;
import io.confluent.kafkarest.entities.v3.GetConsumerGroupLagSummaryResponse;
import io.confluent.kafkarest.entities.v3.Resource;
import io.confluent.kafkarest.entities.v3.Resource.Relationship;
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

@Path("/v3/clusters/{clusterId}/consumer-groups/{consumerGroupId}/lag-summary")
@ResourceName("api.v3.consumer-group-lag-summary.*")
public final class ConsumerGroupLagSummariesResource {

  private final Provider<ConsumerGroupLagSummaryManager> consumerGroupLagSummaryManager;
  private final CrnFactory crnFactory;
  private final UrlFactory urlFactory;

  @Inject
  public ConsumerGroupLagSummariesResource(
      Provider<ConsumerGroupLagSummaryManager> consumerGroupLagSummaryManager,
      CrnFactory crnFactory,
      UrlFactory urlFactory) {
    this.consumerGroupLagSummaryManager = requireNonNull(consumerGroupLagSummaryManager);
    this.crnFactory = requireNonNull(crnFactory);
    this.urlFactory = requireNonNull(urlFactory);
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @PerformanceMetric("v3.consumer-group-lag-summary.get")
  @ResourceName("api.v3.consumer-group-lag-summary.get")
  public void getConsumerGroupLagSummary(
      @Suspended AsyncResponse asyncResponse,
      @PathParam("clusterId") String clusterId,
      @PathParam("consumerGroupId") String consumerGroupId) {
    CompletableFuture<GetConsumerGroupLagSummaryResponse> response =
        consumerGroupLagSummaryManager
            .get()
            .getConsumerGroupLagSummary(clusterId, consumerGroupId)
            .thenApply(groupLagSummary -> groupLagSummary.orElseThrow(NotFoundException::new))
            .thenApply(
                groupLagSummary ->
                    GetConsumerGroupLagSummaryResponse.create(
                        toConsumerGroupLagSummaryData(groupLagSummary)));

    AsyncResponses.asyncResume(asyncResponse, response);
  }

  private ConsumerGroupLagSummaryData toConsumerGroupLagSummaryData(
      ConsumerGroupLagSummary groupLagSummary) {
    return ConsumerGroupLagSummaryData.fromConsumerGroupLagSummary(groupLagSummary)
        .setMetadata(
            Resource.Metadata.builder()
                .setSelf(
                    urlFactory.create(
                        "v3",
                        "clusters",
                        groupLagSummary.getClusterId(),
                        "consumer-groups",
                        groupLagSummary.getConsumerGroupId(),
                        "lag-summary"))
                .setResourceName(
                    crnFactory.create(
                        "kafka",
                        groupLagSummary.getClusterId(),
                        "consumer-group",
                        groupLagSummary.getConsumerGroupId(),
                        "lag-summary",
                        null))
                .build())
        .setMaxLagConsumer(
            Relationship.create(
                urlFactory.create(
                    "v3",
                    "clusters",
                    groupLagSummary.getClusterId(),
                    "consumer-groups",
                    groupLagSummary.getConsumerGroupId(),
                    "consumers",
                    groupLagSummary.getMaxLagConsumerId())))
        .setMaxLagPartition(
            Relationship.create(
                urlFactory.create(
                    "v3",
                    "clusters",
                    groupLagSummary.getClusterId(),
                    "topics",
                    groupLagSummary.getMaxLagTopicName(),
                    "partitions",
                    Integer.toString(groupLagSummary.getMaxLagPartitionId()))))
        .build();
  }
}
