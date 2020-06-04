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

import io.confluent.kafkarest.controllers.BrokerManager;
import io.confluent.kafkarest.entities.Broker;
import io.confluent.kafkarest.entities.v3.BrokerData;
import io.confluent.kafkarest.entities.v3.BrokerDataList;
import io.confluent.kafkarest.entities.v3.GetBrokerResponse;
import io.confluent.kafkarest.entities.v3.ListBrokersResponse;
import io.confluent.kafkarest.entities.v3.Resource;
import io.confluent.kafkarest.entities.v3.ResourceCollection;
import io.confluent.kafkarest.resources.AsyncResponses;
import io.confluent.kafkarest.response.CrnFactory;
import io.confluent.kafkarest.response.UrlFactory;
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

@Path("/v3/clusters/{clusterId}/brokers")
public final class BrokersResource {

  private final Provider<BrokerManager> brokerManager;
  private final CrnFactory crnFactory;
  private final UrlFactory urlFactory;

  @Inject
  public BrokersResource(
      Provider<BrokerManager> brokerManager,
      CrnFactory crnFactory,
      UrlFactory urlFactory
  ) {
    this.brokerManager = requireNonNull(brokerManager);
    this.crnFactory = requireNonNull(crnFactory);
    this.urlFactory = requireNonNull(urlFactory);
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public void listBrokers(
      @Suspended AsyncResponse asyncResponse, @PathParam("clusterId") String clusterId) {
    CompletableFuture<ListBrokersResponse> response =
        brokerManager.get()
            .listBrokers(clusterId)
            .thenApply(
                brokers ->
                    ListBrokersResponse.create(
                        BrokerDataList.builder()
                            .setMetadata(
                                ResourceCollection.Metadata.builder()
                                    .setSelf(
                                        urlFactory.create("v3", "clusters", clusterId, "brokers"))
                                    .build())
                            .setData(
                                brokers.stream()
                                    .map(this::toBrokerData)
                                    .collect(Collectors.toList()))
                            .build()));

    AsyncResponses.asyncResume(asyncResponse, response);
  }

  @GET
  @Path("/{brokerId}")
  @Produces(MediaType.APPLICATION_JSON)
  public void getBroker(
      @Suspended AsyncResponse asyncResponse,
      @PathParam("clusterId") String clusterId,
      @PathParam("brokerId") Integer brokerId
  ) {
    CompletableFuture<GetBrokerResponse> response =
        brokerManager.get()
            .getBroker(clusterId, brokerId)
            .thenApply(broker -> broker.orElseThrow(NotFoundException::new))
            .thenApply(broker -> GetBrokerResponse.create(toBrokerData(broker)));

    AsyncResponses.asyncResume(asyncResponse, response);
  }

  private BrokerData toBrokerData(Broker broker) {
    return BrokerData.fromBroker(broker)
        .setMetadata(
            Resource.Metadata.builder()
                .setSelf(
                    urlFactory.create(
                        "v3",
                        "clusters",
                        broker.getClusterId(),
                        "brokers",
                        Integer.toString(broker.getBrokerId())))
                .setResourceName(
                    crnFactory.create(
                        "kafka",
                        broker.getClusterId(),
                        "broker",
                        Integer.toString(broker.getBrokerId())))
                .build())
        .setConfigs(
            Resource.Relationship.create(
                urlFactory.create(
                    "v3",
                    "clusters",
                    broker.getClusterId(),
                    "brokers",
                    Integer.toString(broker.getBrokerId()),
                    "configs")))
        .setPartitionReplicas(
            Resource.Relationship.create(
                urlFactory.create(
                    "v3",
                    "clusters",
                    broker.getClusterId(),
                    "brokers",
                    Integer.toString(broker.getBrokerId()),
                    "partition-replicas")))
        .build();
  }
}
