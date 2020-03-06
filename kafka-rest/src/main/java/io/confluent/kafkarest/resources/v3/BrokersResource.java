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

import io.confluent.kafkarest.Versions;
import io.confluent.kafkarest.controllers.BrokerManager;
import io.confluent.kafkarest.entities.Broker;
import io.confluent.kafkarest.entities.v3.BrokerData;
import io.confluent.kafkarest.entities.v3.CollectionLink;
import io.confluent.kafkarest.entities.v3.GetBrokerResponse;
import io.confluent.kafkarest.entities.v3.ListBrokersResponse;
import io.confluent.kafkarest.entities.v3.Relationship;
import io.confluent.kafkarest.entities.v3.ResourceLink;
import io.confluent.kafkarest.response.UrlFactory;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Response.Status;

@Path("/v3/clusters/{clusterId}/brokers")
public final class BrokersResource {

  private final BrokerManager brokerManager;
  private final UrlFactory urlFactory;

  @Inject
  public BrokersResource(BrokerManager brokerManager, UrlFactory urlFactory) {
    this.brokerManager = Objects.requireNonNull(brokerManager);
    this.urlFactory = Objects.requireNonNull(urlFactory);
  }

  @GET
  @Produces(Versions.JSON_API)
  public void listBrokers(
      @Suspended AsyncResponse asyncResponse, @PathParam("clusterId") String clusterId) {
    CompletableFuture<ListBrokersResponse> response =
        brokerManager.listBrokers(clusterId)
            .thenApply(
                brokers ->
                    new ListBrokersResponse(
                        new CollectionLink(
                            urlFactory.create(
                                "v3", "clusters", clusterId, "brokers"), /* next= */ null),
                        brokers.stream().map(this::toBrokerData).collect(Collectors.toList())));

    AsyncResponses.asyncResume(asyncResponse, response);
  }

  @GET
  @Path("/{brokerId}")
  @Produces(Versions.JSON_API)
  public void getBroker(
      @Suspended AsyncResponse asyncResponse,
      @PathParam("clusterId") String clusterId,
      @PathParam("brokerId") Integer brokerId
  ) {
    CompletableFuture<GetBrokerResponse> response =
        brokerManager.getBroker(clusterId, brokerId)
            .thenApply(
                broker ->
                    broker.map(this::toBrokerData)
                        .map(GetBrokerResponse::new)
                        .orElseThrow(NotFoundException::new));

    AsyncResponses.asyncResume(asyncResponse, response);
  }

  @GET
  @Path("/{brokerId}/configurations")
  @Produces(Versions.JSON_API)
  public void listBrokerConfigurations(
      @PathParam("clusterId") String clusterId, @PathParam("brokerId") String brokerId) {
    throw new WebApplicationException(Status.NOT_IMPLEMENTED);
  }

  @GET
  @Path("/{brokerId}/partition_replicas")
  @Produces(Versions.JSON_API)
  public void listBrokerPartitionReplicas(
      @PathParam("clusterId") String clusterId, @PathParam("brokerId") String brokerId) {
    throw new WebApplicationException(Status.NOT_IMPLEMENTED);
  }

  private BrokerData toBrokerData(Broker broker) {
    ResourceLink links =
        new ResourceLink(
            urlFactory.create(
                "v3",
                "clusters",
                broker.getClusterId(),
                "brokers",
                Integer.toString(broker.getBrokerId())));
    Relationship configurations =
        new Relationship(
            urlFactory.create(
                "v3",
                "clusters",
                broker.getClusterId(),
                "brokers",
                Integer.toString(broker.getBrokerId()),
                "configurations"));
    Relationship partitionReplicas =
        new Relationship(
            urlFactory.create(
                "v3",
                "clusters",
                broker.getClusterId(),
                "brokers",
                Integer.toString(broker.getBrokerId()),
                "partition_replicas"));

    return new BrokerData(
        links,
        broker.getClusterId(),
        broker.getBrokerId(),
        broker.getHost(),
        broker.getPort(),
        broker.getRack(),
        configurations,
        partitionReplicas);
  }
}
