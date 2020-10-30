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

import io.confluent.kafkarest.controllers.ConsumerLagSummaryManager;
import io.confluent.kafkarest.entities.ConsumerLagSummary;
import io.confluent.kafkarest.entities.v3.ConsumerLagSummaryData;
import io.confluent.kafkarest.entities.v3.ConsumerLagSummaryDataList;
import io.confluent.kafkarest.entities.v3.GetConsumerLagSummaryResponse;
import io.confluent.kafkarest.entities.v3.ListConsumerLagSummaryResponse;
import io.confluent.kafkarest.entities.v3.Resource;
import io.confluent.kafkarest.entities.v3.Resource.Relationship;
import io.confluent.kafkarest.entities.v3.ResourceCollection;
import io.confluent.kafkarest.resources.AsyncResponses;
import io.confluent.kafkarest.response.CrnFactory;
import io.confluent.kafkarest.response.UrlFactory;
import io.confluent.rest.annotations.PerformanceMetric;
import java.util.Comparator;
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
import javax.ws.rs.core.MediaType;

@Path(
    "/v3/clusters/{cluster_id}/consumer-groups/{consumer_group_id}/lag_summary"
)
public final class ConsumerLagSummaryResource {

  private final Provider<ConsumerLagSummaryManager> consumerLagSummaryManager;
  private final CrnFactory crnFactory;
  private final UrlFactory urlFactory;

  @Inject
  public ConsumerLagSummaryResource(
      Provider<ConsumerLagSummaryManager> consumerLagSummaryManager,
      CrnFactory crnFactory,
      UrlFactory urlFactory
  ) {
    this.consumerLagSummaryManager = requireNonNull(consumerLagSummaryManager);
    this.crnFactory = requireNonNull(crnFactory);
    this.urlFactory = requireNonNull(urlFactory);
  }

  @GET
  @Path("/{topicName}/partitions/{partitionId}")
  @Produces(MediaType.APPLICATION_JSON)
  @PerformanceMetric("v3.consumer-assignments.get")
  public void getConsumerLagSummary(
      @Suspended AsyncResponse asyncResponse,
      @PathParam("clusterId") String clusterId,
      @PathParam("consumerGroupId") String consumerGroupId,
      @PathParam("consumerId") String consumerId,
      @PathParam("topicName") String topicName,
      @PathParam("partitionId") int partitionId
  ) {
    CompletableFuture<GetConsumerLagSummaryResponse> response =
        consumerLagSummaryManager.get()
            .getConsumerLagSummary(clusterId, consumerGroupId, consumerId, topicName, partitionId)
            .thenApply(lagSummary -> lagSummary.orElseThrow(NotFoundException::new))
            .thenApply(
                lagSummary ->
                    GetConsumerLagSummaryResponse.create(toConsumerLagSummaryData(lagSummary)));

    AsyncResponses.asyncResume(asyncResponse, response);
  }

  private ConsumerLagSummaryData toConsumerLagSummaryData(ConsumerLagSummary lagSummary) {
    return ConsumerLagSummaryData.fromConsumerLagSummary(lagSummary)
        .setMetadata(
            Resource.Metadata.builder()
                .setSelf(
                    urlFactory.create(
                        "v3",
                        "clusters",
                        lagSummary.getClusterId(),
                        "consumer-groups",
                        lagSummary.getConsumerGroupId(),
                        "consumers",
                        lagSummary.getConsumerId(),
                        "assignments",
                        lagSummary.getTopicName(),
                        "partitions",
                        Integer.toString(lagSummary.getPartitionId())))
                .setResourceName(
                    crnFactory.create(
                        "kafka",
                        lagSummary.getClusterId(),
                        "consumer-group",
                        lagSummary.getConsumerGroupId(),
                        "consumer",
                        lagSummary.getConsumerId(),
                        "assignment",
                        lagSummary.getTopicName(),
                        "partition",
                        Integer.toString(lagSummary.getPartitionId())))
                .build())
        .setPartition(
            Relationship.create(
                urlFactory.create(
                    "v3",
                    "clusters",
                    lagSummary.getClusterId(),
                    "topics",
                    lagSummary.getTopicName(),
                    "partitions",
                    Integer.toString(lagSummary.getPartitionId()))))
        .build();
  }
}
