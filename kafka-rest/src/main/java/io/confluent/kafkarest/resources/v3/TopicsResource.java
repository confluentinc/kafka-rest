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

import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;

import io.confluent.kafkarest.Versions;
import io.confluent.kafkarest.controllers.TopicManager;
import io.confluent.kafkarest.entities.Topic;
import io.confluent.kafkarest.entities.v3.ClusterData;
import io.confluent.kafkarest.entities.v3.CollectionLink;
import io.confluent.kafkarest.entities.v3.CreateTopicRequest;
import io.confluent.kafkarest.entities.v3.CreateTopicResponse;
import io.confluent.kafkarest.entities.v3.GetTopicResponse;
import io.confluent.kafkarest.entities.v3.ListTopicsResponse;
import io.confluent.kafkarest.entities.v3.Relationship;
import io.confluent.kafkarest.entities.v3.ResourceLink;
import io.confluent.kafkarest.entities.v3.TopicData;
import io.confluent.kafkarest.resources.AsyncResponses;
import io.confluent.kafkarest.resources.AsyncResponses.AsyncResponseBuilder;
import io.confluent.kafkarest.response.CrnFactory;
import io.confluent.kafkarest.response.UrlFactory;
import java.net.URI;
import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Provider;
import javax.validation.Valid;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

@Path("/v3/clusters/{clusterId}/topics")
public final class TopicsResource {

  private final Provider<TopicManager> topicManager;
  private final CrnFactory crnFactory;
  private final UrlFactory urlFactory;

  @Inject
  public TopicsResource(
      Provider<TopicManager> topicManager, CrnFactory crnFactory, UrlFactory urlFactory) {
    this.topicManager = requireNonNull(topicManager);
    this.crnFactory = requireNonNull(crnFactory);
    this.urlFactory = requireNonNull(urlFactory);
  }

  @GET
  @Produces(Versions.JSON_API)
  public void listTopics(
      @Suspended AsyncResponse asyncResponse, @PathParam("clusterId") String clusterId) {
    CompletableFuture<ListTopicsResponse> response =
        topicManager.get()
            .listTopics(clusterId)
            .thenApply(
                topics ->
                    new ListTopicsResponse(
                        new CollectionLink(
                            urlFactory.create("v3", "clusters", clusterId, "topics"),
                            /* next= */ null),
                        topics.stream()
                            .sorted(Comparator.comparing(Topic::getName))
                            .map(this::toTopicData)
                            .collect(Collectors.toList())));

    AsyncResponses.asyncResume(asyncResponse, response);
  }

  @GET
  @Path("/{topicName}")
  @Produces(Versions.JSON_API)
  public void getTopic(
      @Suspended AsyncResponse asyncResponse,
      @PathParam("clusterId") String clusterId,
      @PathParam("topicName") String topicName
  ) {
    CompletableFuture<GetTopicResponse> response =
        topicManager.get()
            .getTopic(clusterId, topicName)
            .thenApply(topic -> topic.orElseThrow(NotFoundException::new))
            .thenApply(topic -> new GetTopicResponse(toTopicData(topic)));

    AsyncResponses.asyncResume(asyncResponse, response);
  }

  @POST
  @Consumes(Versions.JSON_API)
  @Produces(Versions.JSON_API)
  public void createTopic(
      @Suspended AsyncResponse asyncResponse,
      @PathParam("clusterId") String clusterId,
      @Valid CreateTopicRequest request
  ) {
    String topicName = request.getData().getAttributes().getTopicName();
    int partitionsCount =  request.getData().getAttributes().getPartitionsCount();
    short replicationFactor = request.getData().getAttributes().getReplicationFactor();
    Map<String, String> configs = request.getData().getAttributes().getConfigs();

    TopicData topicData =
        toTopicData(
            Topic.create(
                clusterId,
                topicName,
                /* partitions= */ emptyList(),
                replicationFactor,
                /* isInternal= */ false));

    CompletableFuture<CreateTopicResponse> response =
        topicManager.get()
            .createTopic(clusterId, topicName, partitionsCount, replicationFactor, configs)
            .thenApply(none -> new CreateTopicResponse(topicData));

    AsyncResponseBuilder.from(
        Response.status(Status.CREATED).location(URI.create(topicData.getLinks().getSelf())))
        .entity(response)
        .asyncResume(asyncResponse);
  }

  @DELETE
  @Path("/{topicName}")
  @Produces(Versions.JSON_API)
  public void deleteTopic(
      @Suspended AsyncResponse asyncResponse,
      @PathParam("clusterId") String clusterId,
      @PathParam("topicName") String topicName
  ) {
    CompletableFuture<Void> response = topicManager.get().deleteTopic(clusterId, topicName);

    AsyncResponseBuilder.from(Response.status(Status.NO_CONTENT))
        .entity(response)
        .asyncResume(asyncResponse);
  }

  private TopicData toTopicData(Topic topic) {
    Relationship configs =
        new Relationship(urlFactory.create(
            "v3",
            "clusters",
            topic.getClusterId(),
            "topics",
            topic.getName(),
            "configs"));
    Relationship partitions =
        new Relationship(urlFactory.create(
            "v3",
            "clusters",
            topic.getClusterId(),
            "topics",
            topic.getName(),
            "partitions"));

    return new TopicData(
        crnFactory.create(
            ClusterData.ELEMENT_TYPE,
            topic.getClusterId(),
            TopicData.ELEMENT_TYPE,
            topic.getName()),
        new ResourceLink(
            urlFactory.create("v3", "clusters", topic.getClusterId(), "topics", topic.getName())),
        topic.getClusterId(),
        topic.getName(),
        topic.isInternal(),
        topic.getReplicationFactor(),
        configs,
        partitions);
  }
}
