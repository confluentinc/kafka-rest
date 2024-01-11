/*
 * Copyright 2021 Confluent Inc.
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
import io.confluent.kafkarest.controllers.TopicManager;
import io.confluent.kafkarest.entities.TopicConfig;
import io.confluent.kafkarest.entities.v3.ListTopicConfigsResponse;
import io.confluent.kafkarest.entities.v3.ResourceCollection;
import io.confluent.kafkarest.entities.v3.TopicConfigDataList;
import io.confluent.kafkarest.extension.ResourceAccesslistFeature.ResourceName;
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
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.MediaType;

@Path("/v3/clusters/{clusterId}/topics/-/configs")
@ResourceName("api.v3.topic-configs.*")
public final class ListAllTopicsConfigsAction {

  private final Provider<TopicManager> topicManager;
  private final Provider<TopicConfigManager> topicConfigManager;
  private final CrnFactory crnFactory;
  private final UrlFactory urlFactory;

  @Inject
  public ListAllTopicsConfigsAction(
      Provider<TopicManager> topicManager,
      Provider<TopicConfigManager> topicConfigManager,
      CrnFactory crnFactory,
      UrlFactory urlFactory) {
    this.topicManager = requireNonNull(topicManager);
    this.topicConfigManager = requireNonNull(topicConfigManager);
    this.crnFactory = requireNonNull(crnFactory);
    this.urlFactory = requireNonNull(urlFactory);
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @PerformanceMetric("v3.topics.configs.list")
  @ResourceName("api.v3.topic-configs.list")
  public void listTopicConfigs(
      @Suspended AsyncResponse asyncResponse, @PathParam("clusterId") String clusterId) {
    // have to resolve dependencies here in request scope
    TopicConfigManager resolvedTopicConfigManager = topicConfigManager.get();
    CompletableFuture<ListTopicConfigsResponse> response =
        topicManager
            .get()
            .listTopics(clusterId)
            .thenCompose(
                topics ->
                    resolvedTopicConfigManager
                        .listTopicConfigs(
                            clusterId,
                            topics.stream()
                                .map(topic -> topic.getName())
                                .collect(Collectors.toList()))
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
                                                        "-",
                                                        "configs"))
                                                .build())
                                        .setData(
                                            configs.values().stream()
                                                .flatMap(
                                                    topicConfigs ->
                                                        topicConfigs.stream()
                                                            .sorted(
                                                                Comparator.comparing(
                                                                    TopicConfig::getName)))
                                                .map(
                                                    topicConfig ->
                                                        TopicConfigsResource.toTopicConfigData(
                                                            topicConfig, crnFactory, urlFactory))
                                                .collect(Collectors.toList()))
                                        .build())));

    AsyncResponses.asyncResume(asyncResponse, response);
  }
}
