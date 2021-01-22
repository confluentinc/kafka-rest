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

import io.confluent.kafkarest.controllers.ConsumerGroupLagManager;
import io.confluent.kafkarest.entities.ConsumerGroupLag;
import io.confluent.kafkarest.entities.v3.ConsumerGroupLagData;
import io.confluent.kafkarest.entities.v3.GetConsumerGroupLagResponse;
import io.confluent.kafkarest.entities.v3.Resource;
import io.confluent.kafkarest.entities.v3.Resource.Relationship;
import io.confluent.kafkarest.extension.ResourceBlocklistFeature.ResourceName;
import io.confluent.kafkarest.resources.AsyncResponses;
import io.confluent.kafkarest.response.CrnFactory;
import io.confluent.kafkarest.response.UrlFactory;
import io.confluent.rest.annotations.PerformanceMetric;
import java.util.concurrent.CompletableFuture;
import javax.inject.Inject;
import javax.inject.Provider;
import javax.management.relation.Relation;
import javax.ws.rs.GET;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.MediaType;

@Path("/v3/clusters/{clusterId}/consumer-groups/{consumerGroupId}/lag")
@ResourceName("api.v3.consumer-group-lag.*")
public final class ConsumerGroupLagsResource {

  private final Provider<ConsumerGroupLagManager> consumerGroupLagManager;
  private final CrnFactory crnFactory;
  private final UrlFactory urlFactory;

  @Inject
  public ConsumerGroupLagsResource(
      Provider<ConsumerGroupLagManager> consumerGroupLagManager,
      CrnFactory crnFactory,
      UrlFactory urlFactory
  ) {
    this.consumerGroupLagManager = requireNonNull(consumerGroupLagManager);
    this.crnFactory = requireNonNull(crnFactory);
    this.urlFactory = requireNonNull(urlFactory);
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @PerformanceMetric("v3.consumer-group-lag.get")
  @ResourceName("api.v3.consumer-group-lag.get")
  public void getConsumerGroupLag(
      @Suspended AsyncResponse asyncResponse,
      @PathParam("clusterId") String clusterId,
      @PathParam("consumerGroupId") String consumerGroupId
  ) {
    CompletableFuture<GetConsumerGroupLagResponse> response =
        consumerGroupLagManager.get()
            .getConsumerGroupLag(clusterId, consumerGroupId)
            .thenApply(groupLag -> groupLag.orElseThrow(NotFoundException::new))
            .thenApply(
                groupLag ->
                    GetConsumerGroupLagResponse.create(toConsumerGroupLagData(groupLag)));

    AsyncResponses.asyncResume(asyncResponse, response);
  }

  private ConsumerGroupLagData toConsumerGroupLagData(ConsumerGroupLag groupLag) {
    return ConsumerGroupLagData.fromConsumerGroupLag(groupLag)
        .setMetadata(
            Resource.Metadata.builder()
                .setSelf(
                    urlFactory.create(
                        "v3",
                        "clusters",
                        groupLag.getClusterId(),
                        "consumer-groups",
                        groupLag.getConsumerGroupId(),
                        "lag"))
                .setResourceName(
                    crnFactory.create(
                        "kafka",
                        groupLag.getClusterId(),
                        "consumer-group",
                        groupLag.getConsumerGroupId(),
                        "lag",
                        null))
                .build())
        .setMaxLagConsumer(
            Relationship.create(
                urlFactory.create(
                    "v3",
                    "clusters",
                    groupLag.getClusterId(),
                    "consumer-groups",
                    groupLag.getConsumerGroupId(),
                    "consumers",
                    groupLag.getMaxLagConsumerId())))
        .setMaxLagPartition(
            Relationship.create(
                urlFactory.create(
                    "v3",
                    "clusters",
                    groupLag.getClusterId(),
                    "topics",
                    groupLag.getMaxLagTopicName(),
                    "partitions",
                    Integer.toString(groupLag.getMaxLagPartitionId()))))
        .build();
  }
}
