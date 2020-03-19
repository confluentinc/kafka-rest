/*
 * Copyright 2018 Confluent Inc.
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
import io.confluent.kafkarest.controllers.TopicConfigurationManager;
import io.confluent.kafkarest.entities.TopicConfiguration;
import io.confluent.kafkarest.entities.v3.ClusterData;
import io.confluent.kafkarest.entities.v3.CollectionLink;
import io.confluent.kafkarest.entities.v3.GetTopicConfigurationResponse;
import io.confluent.kafkarest.entities.v3.ListTopicConfigurationsResponse;
import io.confluent.kafkarest.entities.v3.ResourceLink;
import io.confluent.kafkarest.entities.v3.TopicConfigurationData;
import io.confluent.kafkarest.entities.v3.TopicData;
import io.confluent.kafkarest.entities.v3.UpdateTopicConfigurationRequest;
import io.confluent.kafkarest.resources.v3.AsyncResponses.AsyncResponseBuilder;
import io.confluent.kafkarest.response.CrnFactory;
import io.confluent.kafkarest.response.UrlFactory;
import java.util.Comparator;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.validation.Valid;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

@Path("/v3/clusters/{clusterId}/topics/{topicName}/configurations")
public final class TopicConfigurationsResource {

  private final TopicConfigurationManager topicConfigurationManager;
  private final CrnFactory crnFactory;
  private final UrlFactory urlFactory;

  @Inject
  public TopicConfigurationsResource(
      TopicConfigurationManager topicConfigurationManager,
      CrnFactory crnFactory,
      UrlFactory urlFactory) {
    this.topicConfigurationManager = Objects.requireNonNull(topicConfigurationManager);
    this.crnFactory = Objects.requireNonNull(crnFactory);
    this.urlFactory = Objects.requireNonNull(urlFactory);
  }

  @GET
  @Produces(Versions.JSON_API)
  public void listTopicConfigurations(
      @Suspended AsyncResponse asyncResponse,
      @PathParam("clusterId") String clusterId,
      @PathParam("topicName") String topicName) {
    CompletableFuture<ListTopicConfigurationsResponse> response =
        topicConfigurationManager.listTopicConfigurations(clusterId, topicName)
            .thenApply(
                configurations ->
                    new ListTopicConfigurationsResponse(
                        new CollectionLink(
                            urlFactory.create(
                                "v3", "clusters", clusterId, "topics", topicName, "configurations"),
                            /* next= */ null),
                        configurations.stream()
                            .sorted(Comparator.comparing(TopicConfiguration::getName))
                            .map(this::toTopicConfigurationData)
                            .collect(Collectors.toList())));

    AsyncResponses.asyncResume(asyncResponse, response);
  }

  @GET
  @Path("/{name}")
  @Produces(Versions.JSON_API)
  public void getTopicConfiguration(
      @Suspended AsyncResponse asyncResponse,
      @PathParam("clusterId") String clusterId,
      @PathParam("topicName") String topicName,
      @PathParam("name") String name
  ) {
    CompletableFuture<GetTopicConfigurationResponse> response =
        topicConfigurationManager.getTopicConfiguration(clusterId, topicName, name)
            .thenApply(topic -> topic.orElseThrow(NotFoundException::new))
            .thenApply(topic -> new GetTopicConfigurationResponse(toTopicConfigurationData(topic)));

    AsyncResponses.asyncResume(asyncResponse, response);
  }

  @PUT
  @Path("/{name}")
  @Consumes(Versions.JSON_API)
  public void updateTopicConfiguration(
      @Suspended AsyncResponse asyncResponse,
      @PathParam("clusterId") String clusterId,
      @PathParam("topicName") String topicName,
      @PathParam("name") String name,
      @Valid UpdateTopicConfigurationRequest request
  ) {
    String newValue = request.getData().getAttributes().getValue();

    CompletableFuture<Void> response =
        topicConfigurationManager.updateTopicConfiguration(clusterId, topicName, name, newValue);

    AsyncResponseBuilder.from(Response.status(Status.NO_CONTENT))
        .entity(response)
        .asyncResume(asyncResponse);
  }

  @DELETE
  @Path("/{name}")
  public void resetTopicConfiguration(
      @Suspended AsyncResponse asyncResponse,
      @PathParam("clusterId") String clusterId,
      @PathParam("topicName") String topicName,
      @PathParam("name") String name
  ) {
    CompletableFuture<Void> response =
        topicConfigurationManager.resetTopicConfiguration(clusterId, topicName, name);

    AsyncResponseBuilder.from(Response.status(Status.NO_CONTENT))
        .entity(response)
        .asyncResume(asyncResponse);
  }

  private TopicConfigurationData toTopicConfigurationData(TopicConfiguration topicConfiguration) {
    return new TopicConfigurationData(
        crnFactory.create(
            ClusterData.ELEMENT_TYPE,
            topicConfiguration.getClusterId(),
            TopicData.ELEMENT_TYPE,
            topicConfiguration.getTopicName(),
            TopicConfigurationData.ELEMENT_TYPE,
            topicConfiguration.getName()),
        new ResourceLink(
            urlFactory.create(
                "v3",
                "clusters",
                topicConfiguration.getClusterId(),
                "topics",
                topicConfiguration.getTopicName(),
                "configurations",
                topicConfiguration.getName())),
        topicConfiguration.getClusterId(),
        topicConfiguration.getTopicName(),
        topicConfiguration.getName(),
        topicConfiguration.getValue(),
        topicConfiguration.isDefault(),
        topicConfiguration.isReadOnly(),
        topicConfiguration.isSensitive());
  }
}
