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
import io.confluent.kafkarest.extension.ResourceAccesslistFeature.ResourceName;
import io.confluent.kafkarest.resources.AsyncResponses;
import io.confluent.kafkarest.resources.AsyncResponses.AsyncResponseBuilder;
import io.confluent.kafkarest.response.CrnFactory;
import io.confluent.kafkarest.response.UrlFactory;
import io.confluent.rest.annotations.PerformanceMetric;
import jakarta.inject.Inject;
import jakarta.inject.Provider;
import jakarta.validation.Valid;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.NotFoundException;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.container.AsyncResponse;
import jakarta.ws.rs.container.Suspended;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.Response.Status;
import java.util.Comparator;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

@Path("/v3/clusters/{clusterId}/topics/{topicName}/configs")
@ResourceName("api.v3.topic-configs.*")
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
  @PerformanceMetric("v3.topics.configs.list")
  @ResourceName("api.v3.topic-configs.list")
  public void listTopicConfigs(
      @Suspended AsyncResponse asyncResponse,
      @PathParam("clusterId") String clusterId,
      @PathParam("topicName") String topicName) {
    CompletableFuture<ListTopicConfigsResponse> response =
        topicConfigManager
            .get()
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
                                    .map(
                                        topicConfig ->
                                            toTopicConfigData(topicConfig, crnFactory, urlFactory))
                                    .collect(Collectors.toList()))
                            .build()));

    AsyncResponses.asyncResume(asyncResponse, response);
  }

  @GET
  @Path("/{name}")
  @Produces(MediaType.APPLICATION_JSON)
  @PerformanceMetric("v3.topics.configs.get")
  @ResourceName("api.v3.topic-configs.get")
  public void getTopicConfig(
      @Suspended AsyncResponse asyncResponse,
      @PathParam("clusterId") String clusterId,
      @PathParam("topicName") String topicName,
      @PathParam("name") String name) {
    CompletableFuture<GetTopicConfigResponse> response =
        topicConfigManager
            .get()
            .getTopicConfig(clusterId, topicName, name)
            .thenApply(topic -> topic.orElseThrow(NotFoundException::new))
            .thenApply(
                topic ->
                    GetTopicConfigResponse.create(
                        toTopicConfigData(topic, crnFactory, urlFactory)));

    AsyncResponses.asyncResume(asyncResponse, response);
  }

  @PUT
  @Path("/{name}")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @PerformanceMetric("v3.topics.configs.update")
  @ResourceName("api.v3.topic-configs.update")
  public void updateTopicConfig(
      @Suspended AsyncResponse asyncResponse,
      @PathParam("clusterId") String clusterId,
      @PathParam("topicName") String topicName,
      @PathParam("name") String name,
      @Valid UpdateTopicConfigRequest request) {
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
  @PerformanceMetric("v3.topics.configs.delete")
  @ResourceName("api.v3.topic-configs.delete")
  public void resetTopicConfig(
      @Suspended AsyncResponse asyncResponse,
      @PathParam("clusterId") String clusterId,
      @PathParam("topicName") String topicName,
      @PathParam("name") String name) {
    CompletableFuture<Void> response =
        topicConfigManager.get().resetTopicConfig(clusterId, topicName, name);

    AsyncResponseBuilder.from(Response.status(Status.NO_CONTENT))
        .entity(response)
        .asyncResume(asyncResponse);
  }

  public static TopicConfigData toTopicConfigData(
      TopicConfig topicConfig, CrnFactory crnFactory, UrlFactory urlFactory) {
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
