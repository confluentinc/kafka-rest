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

import io.confluent.kafkarest.Versions;
import io.confluent.kafkarest.controllers.BrokerManager;
import io.confluent.kafkarest.entities.Broker;
import io.confluent.kafkarest.entities.v3.BrokerData;
import io.confluent.kafkarest.entities.v3.ClusterData;
import io.confluent.kafkarest.entities.v3.CollectionLink;
import io.confluent.kafkarest.entities.v3.GetBrokerResponse;
import io.confluent.kafkarest.entities.v3.ListBrokersResponse;
import io.confluent.kafkarest.entities.v3.Relationship;
import io.confluent.kafkarest.entities.v3.ResourceLink;
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

@Path("/v3/clusters/{clusterId}/brokers")
public final class BrokersResource {

  private final Provider<BrokerManager> brokerManager;
  private final CrnFactory crnFactory;
  private final UrlFactory urlFactory;

  @Inject
  public BrokersResource(
      Provider<BrokerManager> brokerManager, CrnFactory crnFactory, UrlFactory urlFactory) {
    this.brokerManager = requireNonNull(brokerManager);
    this.crnFactory = requireNonNull(crnFactory);
    this.urlFactory = requireNonNull(urlFactory);
  }

  @GET
  @Produces(Versions.JSON_API)
  public void listBrokers(
      @Suspended AsyncResponse asyncResponse, @PathParam("clusterId") String clusterId) {
    CompletableFuture<ListBrokersResponse> response =
        brokerManager.get()
            .listBrokers(clusterId)
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
        brokerManager.get()
            .getBroker(clusterId, brokerId)
            .thenApply(broker -> broker.orElseThrow(NotFoundException::new))
            .thenApply(broker -> new GetBrokerResponse(toBrokerData(broker)));

    AsyncResponses.asyncResume(asyncResponse, response);
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
    Relationship configs =
        new Relationship(
            urlFactory.create(
                "v3",
                "clusters",
                broker.getClusterId(),
                "brokers",
                Integer.toString(broker.getBrokerId()),
                "configs"));
    Relationship partitionReplicas =
        new Relationship(
            urlFactory.create(
                "v3",
                "clusters",
                broker.getClusterId(),
                "brokers",
                Integer.toString(broker.getBrokerId()),
                "partition-replicas"));

    return new BrokerData(
        crnFactory.create(
            ClusterData.ELEMENT_TYPE,
            broker.getClusterId(),
            BrokerData.ELEMENT_TYPE,
            Integer.toString(broker.getBrokerId())),
        links,
        broker.getClusterId(),
        broker.getBrokerId(),
        broker.getHost(),
        broker.getPort(),
        broker.getRack(),
        configs,
        partitionReplicas);
  }
}
