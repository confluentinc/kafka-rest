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

import io.confluent.kafkarest.controllers.ConsumerLagManager;
import io.confluent.kafkarest.entities.ConsumerLag;
import io.confluent.kafkarest.entities.v3.ConsumerLagData;
import io.confluent.kafkarest.entities.v3.GetConsumerLagResponse;
import io.confluent.kafkarest.entities.v3.Resource;
import io.confluent.kafkarest.extension.ResourceBlocklistFeature.ResourceName;
import io.confluent.kafkarest.resources.AsyncResponses;
import io.confluent.kafkarest.response.CrnFactory;
import io.confluent.kafkarest.response.UrlFactory;
import io.confluent.rest.annotations.PerformanceMetric;
import java.util.concurrent.CompletableFuture;
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

@Path("/v3/clusters/{clusterId}/topics/{topicName}/partitions/{partitionId}/lags/{consumerGroupId}")
@ResourceName("api.v3.consumer-lags.*")
public final class GetConsumerLagResource {

  private final Provider<ConsumerLagManager> consumerLagManager;
  private final CrnFactory crnFactory;
  private final UrlFactory urlFactory;

  @Inject
  public GetConsumerLagResource(
      Provider<ConsumerLagManager> consumerLagManager,
      CrnFactory crnFactory,
      UrlFactory urlFactory
  ) {
    this.consumerLagManager = requireNonNull(consumerLagManager);
    this.crnFactory = requireNonNull(crnFactory);
    this.urlFactory = requireNonNull(urlFactory);
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @PerformanceMetric("v3.consumer-lags.get")
  @ResourceName("api.v3.consumer-lags.get")
  public void getConsumerLag(
      @Suspended AsyncResponse asyncResponse,
      @PathParam("clusterId") String clusterId,
      @PathParam("topicName") String topicName,
      @PathParam("partitionId") Integer partitionId,
      @PathParam("consumerGroupId") String consumerGroupId
  ) {
    CompletableFuture<GetConsumerLagResponse> response =
        consumerLagManager.get()
            .getConsumerLag(clusterId, topicName, partitionId, consumerGroupId)
            .thenApply(lag -> lag.orElseThrow(NotFoundException::new))
            .thenApply(
                lag ->
                    GetConsumerLagResponse.create(toConsumerLagData(lag, urlFactory, crnFactory)));

    AsyncResponses.asyncResume(asyncResponse, response);
  }

  // static and accepts UrlFactory and CrnFactory so that ListConsumerLagResource can also use it

  static ConsumerLagData toConsumerLagData(
      ConsumerLag lag, UrlFactory urlFactory, CrnFactory crnFactory) {
    return ConsumerLagData.fromConsumerLag(lag)
        .setMetadata(
            Resource.Metadata.builder()
                .setSelf(
                    urlFactory.create(
                        "v3",
                        "clusters",
                        lag.getClusterId(),
                        "topics",
                        lag.getTopicName(),
                        "partitions",
                        Integer.toString(lag.getPartitionId()),
                        "lags",
                        lag.getConsumerGroupId()))
                .setResourceName(
                    crnFactory.create(
                        "kafka",
                        lag.getClusterId(),
                        "topic",
                        lag.getTopicName(),
                        "partition",
                        Integer.toString(lag.getPartitionId()),
                        "lag",
                        lag.getConsumerGroupId()))
                .build())
        .build();
  }

  //  private ConsumerLagData toConsumerLagData(ConsumerLag lag) {
  //    return ConsumerLagData.fromConsumerLag(lag)
  //        .setMetadata(
  //            Resource.Metadata.builder()
  //                .setSelf(
  //                    urlFactory.create(
  //                        "v3",
  //                        "clusters",
  //                        lag.getClusterId(),
  //                        "topics",
  //                        lag.getTopicName(),
  //                        "partitions",
  //                        Integer.toString(lag.getPartitionId()),
  //                        "lags",
  //                        lag.getConsumerGroupId()))
  //                .setResourceName(
  //                    crnFactory.create(
  //                        "kafka",
  //                        lag.getClusterId(),
  //                        "topic",
  //                        lag.getTopicName(),
  //                        "partition",
  //                        Integer.toString(lag.getPartitionId()),
  //                        "lag",
  //                        lag.getConsumerGroupId()))
  //                .build())
  //        .build();
  //  }
}
