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

package io.confluent.kafkarest.resources.v2;

import static java.util.Objects.requireNonNull;

import io.confluent.kafkarest.Errors;
import io.confluent.kafkarest.Versions;
import io.confluent.kafkarest.controllers.TopicConfigManager;
import io.confluent.kafkarest.controllers.TopicManager;
import io.confluent.kafkarest.entities.Topic;
import io.confluent.kafkarest.entities.v2.GetTopicResponse;
import io.confluent.kafkarest.resources.AsyncResponses;
import io.confluent.rest.annotations.PerformanceMetric;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Provider;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;

@Path("/topics")
@Consumes({Versions.KAFKA_V2_JSON})
@Produces({Versions.KAFKA_V2_JSON})
public final class TopicsResource {

  private final Provider<TopicManager> topicManagerProvider;
  private final Provider<TopicConfigManager> topicConfigManagerProvider;

  @Inject
  public TopicsResource(
      Provider<TopicManager> topicManagerProvider,
      Provider<TopicConfigManager> topicConfigManagerProvider) {
    this.topicManagerProvider = requireNonNull(topicManagerProvider);
    this.topicConfigManagerProvider = requireNonNull(topicConfigManagerProvider);
  }

  @GET
  @PerformanceMetric("topics.list+v2")
  public void list(@Suspended AsyncResponse asyncResponse) {
    TopicManager topicManager = topicManagerProvider.get();

    CompletableFuture<List<String>> response =
        topicManager.listLocalTopics()
            .thenApply(topics -> topics.stream().map(Topic::getName).collect(Collectors.toList()));

    AsyncResponses.asyncResume(asyncResponse, response);
  }

  @GET
  @Path("/{topic}")
  @PerformanceMetric("topic.get+v2")
  public void getTopic(
      @Suspended AsyncResponse asyncResponse, @PathParam("topic") String topicName) {
    TopicManager topicManager = topicManagerProvider.get();
    TopicConfigManager topicConfigManager = topicConfigManagerProvider.get();

    CompletableFuture<Topic> topicFuture =
        topicManager.getLocalTopic(topicName)
            .thenApply(topic -> topic.orElseThrow(Errors::topicNotFoundException));
    CompletableFuture<GetTopicResponse> response =
        topicFuture.thenCompose(
            topic -> topicConfigManager.listTopicConfigs(topic.getClusterId(), topicName))
            .thenCombine(
                topicFuture,
                (configs, topic) -> GetTopicResponse.fromTopic(topic, configs));

    AsyncResponses.asyncResume(asyncResponse, response);
  }
}
