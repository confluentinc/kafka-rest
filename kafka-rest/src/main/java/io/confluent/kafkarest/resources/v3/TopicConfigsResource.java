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

import io.confluent.kafkarest.controllers.TopicConfigManager;
import io.confluent.kafkarest.entities.TopicConfig;
import io.confluent.kafkarest.entities.v3.GetTopicConfigResponse;
import io.confluent.kafkarest.entities.v3.ListTopicConfigsResponse;
import io.confluent.kafkarest.entities.v3.Resource;
import io.confluent.kafkarest.entities.v3.ResourceCollection;
import io.confluent.kafkarest.entities.v3.TopicConfigData;
import io.confluent.kafkarest.entities.v3.TopicConfigDataList;
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
import javax.ws.rs.core.MediaType;
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
  @Produces(MediaType.APPLICATION_JSON)
  public void listTopicConfigs(
      @Suspended AsyncResponse asyncResponse,
      @PathParam("clusterId") String clusterId,
      @PathParam("topicName") String topicName
  ) {
    CompletableFuture<ListTopicConfigsResponse> response =
        topicConfigManager.get()
            .listTopicConfigs(clusterId, topicName)
            .thenApply(
                configs ->
                    ListTopicConfigsResponse.create(
                        TopicConfigDataList.builder()
                            .setMetadata(
                                ResourceCollection.Metadata.builder()
                                    .setSelf(
                                        urlFactory.create(
                                            "v3",
                                            "clusters",
                                            clusterId,
                                            "topics",
                                            topicName,
                                            "configs"))
                                    .build())
                            .setData(
                                configs.stream()
                                    .sorted(Comparator.comparing(TopicConfig::getName))
                                    .map(this::toTopicConfigData)
                                    .collect(Collectors.toList()))
                            .build()));

    AsyncResponses.asyncResume(asyncResponse, response);
  }

  @GET
  @Path("/{name}")
  @Produces(MediaType.APPLICATION_JSON)
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
            .thenApply(topic -> GetTopicConfigResponse.create(toTopicConfigData(topic)));

    AsyncResponses.asyncResume(asyncResponse, response);
  }

  @PUT
  @Path("/{name}")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public void updateTopicConfig(
      @Suspended AsyncResponse asyncResponse,
      @PathParam("clusterId") String clusterId,
      @PathParam("topicName") String topicName,
      @PathParam("name") String name,
      @Valid UpdateTopicConfigRequest request
  ) {
    String newValue = request.getValue().orElse(null);

    CompletableFuture<Void> response =
        topicConfigManager.get().updateTopicConfig(clusterId, topicName, name, newValue);

    AsyncResponseBuilder.from(Response.status(Status.NO_CONTENT))
        .entity(response)
        .asyncResume(asyncResponse);
  }

  @DELETE
  @Path("/{name}")
  @Produces(MediaType.APPLICATION_JSON)
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
    return TopicConfigData.fromTopicConfig(topicConfig)
        .setMetadata(
            Resource.Metadata.builder()
                .setSelf(
                    urlFactory.create(
                        "v3",
                        "clusters",
                        topicConfig.getClusterId(),
                        "topics",
                        topicConfig.getTopicName(),
                        "configs",
                        topicConfig.getName()))
                .setResourceName(
                    crnFactory.create(
                        "kafka",
                        topicConfig.getClusterId(),
                        "topic",
                        topicConfig.getTopicName(),
                        "config",
                        topicConfig.getName()))
                .build())
        .build();
  }
}
