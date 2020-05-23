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
import io.confluent.kafkarest.controllers.TopicConfigManager;
import io.confluent.kafkarest.entities.TopicConfig;
import io.confluent.kafkarest.entities.v3.ClusterData;
import io.confluent.kafkarest.entities.v3.CollectionLink;
import io.confluent.kafkarest.entities.v3.ConfigSynonymData;
import io.confluent.kafkarest.entities.v3.GetTopicConfigResponse;
import io.confluent.kafkarest.entities.v3.ListTopicConfigsResponse;
import io.confluent.kafkarest.entities.v3.ResourceLink;
import io.confluent.kafkarest.entities.v3.TopicConfigData;
import io.confluent.kafkarest.entities.v3.TopicData;
import io.confluent.kafkarest.entities.v3.UpdateTopicConfigRequest;
import io.confluent.kafkarest.resources.AsyncResponses;
import io.confluent.kafkarest.resources.AsyncResponses.AsyncResponseBuilder;
import io.confluent.kafkarest.response.CrnFactory;
import io.confluent.kafkarest.response.UrlFactory;
import java.util.Comparator;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Provider;
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

@Path("/v3/clusters/{clusterId}/topics/{topicName}/configs")
public final class TopicConfigsResource {

  private final Provider<TopicConfigManager> topicConfigManager;
  private final CrnFactory crnFactory;
  private final UrlFactory urlFactory;

  @Inject
  public TopicConfigsResource(
      Provider<TopicConfigManager> topicConfigManager,
      CrnFactory crnFactory,
      UrlFactory urlFactory) {
    this.topicConfigManager = requireNonNull(topicConfigManager);
    this.crnFactory = requireNonNull(crnFactory);
    this.urlFactory = requireNonNull(urlFactory);
  }

  @GET
  @Produces(Versions.JSON_API)
  public void listTopicConfigs(
      @Suspended AsyncResponse asyncResponse,
      @PathParam("clusterId") String clusterId,
      @PathParam("topicName") String topicName) {
    CompletableFuture<ListTopicConfigsResponse> response =
        topicConfigManager.get()
            .listTopicConfigs(clusterId, topicName)
            .thenApply(
                configs ->
                    new ListTopicConfigsResponse(
                        new CollectionLink(
                            urlFactory.create(
                                "v3", "clusters", clusterId, "topics", topicName, "configs"),
                            /* next= */ null),
                        configs.stream()
                            .sorted(Comparator.comparing(TopicConfig::getName))
                            .map(this::toTopicConfigData)
                            .collect(Collectors.toList())));

    AsyncResponses.asyncResume(asyncResponse, response);
  }

  @GET
  @Path("/{name}")
  @Produces(Versions.JSON_API)
  public void getTopicConfig(
      @Suspended AsyncResponse asyncResponse,
      @PathParam("clusterId") String clusterId,
      @PathParam("topicName") String topicName,
      @PathParam("name") String name
  ) {
    CompletableFuture<GetTopicConfigResponse> response =
        topicConfigManager.get()
            .getTopicConfig(clusterId, topicName, name)
            .thenApply(topic -> topic.orElseThrow(NotFoundException::new))
            .thenApply(topic -> new GetTopicConfigResponse(toTopicConfigData(topic)));

    AsyncResponses.asyncResume(asyncResponse, response);
  }

  @PUT
  @Path("/{name}")
  @Consumes(Versions.JSON_API)
  @Produces(Versions.JSON_API)
  public void updateTopicConfig(
      @Suspended AsyncResponse asyncResponse,
      @PathParam("clusterId") String clusterId,
      @PathParam("topicName") String topicName,
      @PathParam("name") String name,
      @Valid UpdateTopicConfigRequest request
  ) {
    String newValue = request.getData().getAttributes().getValue();

    CompletableFuture<Void> response =
        topicConfigManager.get().updateTopicConfig(clusterId, topicName, name, newValue);

    AsyncResponseBuilder.from(Response.status(Status.NO_CONTENT))
        .entity(response)
        .asyncResume(asyncResponse);
  }

  @DELETE
  @Path("/{name}")
  @Produces(Versions.JSON_API)
  public void resetTopicConfig(
      @Suspended AsyncResponse asyncResponse,
      @PathParam("clusterId") String clusterId,
      @PathParam("topicName") String topicName,
      @PathParam("name") String name
  ) {
    CompletableFuture<Void> response =
        topicConfigManager.get().resetTopicConfig(clusterId, topicName, name);

    AsyncResponseBuilder.from(Response.status(Status.NO_CONTENT))
        .entity(response)
        .asyncResume(asyncResponse);
  }

  private TopicConfigData toTopicConfigData(TopicConfig topicConfig) {
    return new TopicConfigData(
        crnFactory.create(
            ClusterData.ELEMENT_TYPE,
            topicConfig.getClusterId(),
            TopicData.ELEMENT_TYPE,
            topicConfig.getTopicName(),
            TopicConfigData.ELEMENT_TYPE,
            topicConfig.getName()),
        new ResourceLink(
            urlFactory.create(
                "v3",
                "clusters",
                topicConfig.getClusterId(),
                "topics",
                topicConfig.getTopicName(),
                "configs",
                topicConfig.getName())),
        topicConfig.getClusterId(),
        topicConfig.getTopicName(),
        topicConfig.getName(),
        topicConfig.getValue(),
        topicConfig.isDefault(),
        topicConfig.isReadOnly(),
        topicConfig.isSensitive(),
        topicConfig.getSource(),
        topicConfig.getSynonyms().stream()
            .map(ConfigSynonymData::fromConfigSynonym)
            .collect(Collectors.toList()));
  }
}
