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
import static java.util.Collections.emptySet;
import static java.util.Objects.requireNonNull;

import io.confluent.kafkarest.Errors;
import io.confluent.kafkarest.controllers.TopicManager;
import io.confluent.kafkarest.entities.Topic;
import io.confluent.kafkarest.entities.v3.CreateTopicRequest;
import io.confluent.kafkarest.entities.v3.CreateTopicRequest.ConfigEntry;
import io.confluent.kafkarest.entities.v3.CreateTopicResponse;
import io.confluent.kafkarest.entities.v3.GetTopicResponse;
import io.confluent.kafkarest.entities.v3.ListTopicsResponse;
import io.confluent.kafkarest.entities.v3.PartitionsCountRequest;
import io.confluent.kafkarest.entities.v3.Resource;
import io.confluent.kafkarest.entities.v3.ResourceCollection;
import io.confluent.kafkarest.entities.v3.TopicData;
import io.confluent.kafkarest.entities.v3.TopicDataList;
import io.confluent.kafkarest.extension.ResourceAccesslistFeature.ResourceName;
import io.confluent.kafkarest.resources.AsyncResponses;
import io.confluent.kafkarest.resources.AsyncResponses.AsyncResponseBuilder;
import io.confluent.kafkarest.response.CrnFactory;
import io.confluent.kafkarest.response.UrlFactory;
import io.confluent.rest.annotations.PerformanceMetric;
import java.net.URI;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Provider;
import javax.validation.Valid;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.PATCH;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import org.apache.kafka.common.errors.InvalidTopicException;

@Path("/v3/clusters/{clusterId}/topics")
@ResourceName("api.v3.topics.*")
public final class TopicsResource {

  private final Provider<TopicManager> topicManagerProvider;
  private final CrnFactory crnFactory;
  private final UrlFactory urlFactory;

  @Inject
  public TopicsResource(
      Provider<TopicManager> topicManager, CrnFactory crnFactory, UrlFactory urlFactory) {
    this.topicManagerProvider = requireNonNull(topicManager);
    this.crnFactory = requireNonNull(crnFactory);
    this.urlFactory = requireNonNull(urlFactory);
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @PerformanceMetric("v3.topics.list")
  @ResourceName("api.v3.topics.list")
  public void listTopics(
      @Suspended AsyncResponse asyncResponse,
      @PathParam("clusterId") String clusterId,
      @QueryParam("includeAuthorizedOperations") @DefaultValue("false")
          boolean includeAuthorizedOperations) {
    CompletableFuture<ListTopicsResponse> response =
        topicManagerProvider
            .get()
            .listTopics(clusterId, includeAuthorizedOperations)
            .thenApply(
                topics ->
                    ListTopicsResponse.create(
                        TopicDataList.builder()
                            .setMetadata(
                                ResourceCollection.Metadata.builder()
                                    .setSelf(
                                        urlFactory.create("v3", "clusters", clusterId, "topics"))
                                    .build())
                            .setData(
                                topics.stream()
                                    .sorted(Comparator.comparing(Topic::getName))
                                    .map(this::toTopicData)
                                    .collect(Collectors.toList()))
                            .build()));

    AsyncResponses.asyncResume(asyncResponse, response);
  }

  @GET
  @Path("/{topicName}")
  @Produces(MediaType.APPLICATION_JSON)
  @PerformanceMetric("v3.topics.get")
  @ResourceName("api.v3.topics.get")
  public void getTopic(
      @Suspended AsyncResponse asyncResponse,
      @PathParam("clusterId") String clusterId,
      @PathParam("topicName") String topicName,
      @QueryParam("include_authorized_operations") @DefaultValue("false")
          boolean includeAuthorizedOperations) {
    CompletableFuture<GetTopicResponse> response =
        topicManagerProvider
            .get()
            .getTopic(clusterId, topicName, includeAuthorizedOperations)
            .thenApply(topic -> topic.orElseThrow(NotFoundException::new))
            .thenApply(topic -> GetTopicResponse.create(toTopicData(topic)));

    AsyncResponses.asyncResume(asyncResponse, response);
  }

  @PATCH
  @Path("/{topicName}")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @PerformanceMetric("v3.topics.partitions")
  @ResourceName("api.v3.topics.partitions")
  public void updatePartitionsCount(
      @Suspended AsyncResponse asyncResponse,
      @PathParam("clusterId") String clusterId,
      @PathParam("topicName") String topicName,
      @Valid PartitionsCountRequest partitionsCount) {

    if (partitionsCount == null) {
      throw Errors.invalidPayloadException("Request body is empty. Partitions_count is required.");
    }

    TopicManager topicManager = topicManagerProvider.get();

    CompletableFuture<GetTopicResponse> response =
        topicManager
            .updateTopicPartitionsCount(topicName, partitionsCount.getPartitionsCount())
            .thenCompose(
                nothing ->
                    topicManager
                        .getTopic(clusterId, topicName)
                        .thenApply(topic -> GetTopicResponse.create(toTopicData(topic.get()))));

    AsyncResponses.asyncResume(asyncResponse, response);
  }

  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @PerformanceMetric("v3.topics.create")
  @ResourceName("api.v3.topics.create")
  public void createTopic(
      @Suspended AsyncResponse asyncResponse,
      @PathParam("clusterId") String clusterId,
      @Valid CreateTopicRequest request) {

    if (request == null) {
      throw Errors.invalidPayloadException("Request body is empty. Data is required.");
    }

    String topicName = request.getTopicName();
    try {
      org.apache.kafka.common.internals.Topic.validate(topicName);
    } catch (InvalidTopicException e) {
      throw Errors.invalidPayloadException("Invalid topic name.");
    }

    Optional<Integer> partitionsCount = request.getPartitionsCount();
    Optional<Short> replicationFactor = request.getReplicationFactor();
    Map<Integer, List<Integer>> replicasAssignments = request.getReplicasAssignments();
    Map<String, Optional<String>> configs =
        request.getConfigs().stream()
            .collect(Collectors.toMap(ConfigEntry::getName, ConfigEntry::getValue));

    // The partitions count for the topic is set in three ways, in this order of precedence:
    // 1) If a map of replica assignments is provided, the partition count is calculated.
    // 2) If a partition count is provided, use that value.
    // 3) Otherwise, no value is provided and the topic uses the default partition count.
    Optional<Integer> requestPartitionsCount =
        replicasAssignments.isEmpty()
            ? partitionsCount
            : Optional.of(replicasAssignments.values().size());

    // The replication factor for the topic is set in three ways, in this order of precedence:
    // 1) If a map of replica assignments is provided, the replication factor is calculated.
    // 2) If a replication factor is provided, use that value.
    // 3) Otherwise, no value is provided and the topic uses the default replication factor.
    Optional<Short> requestReplicationFactor =
        replicasAssignments.isEmpty()
            ? replicationFactor
            : Optional.of((short) replicasAssignments.values().iterator().next().size());

    // The CreateTopicResponse is created from TopicData, although this is only really used to
    // build the response in a way compatible with the original implementation. This requires
    // a value for the replication factor, which is supplied, calculated or defaulted to 0.
    // We have no way of knowing the default replication factor in the Kafka broker. Also in case
    // of explicitly specified partition-to-replicas assignments, all partitions should have the
    // same number of replicas.
    short assumedReplicationFactor =
        replicationFactor.orElse(
            replicasAssignments.isEmpty()
                ? 0
                : (short) replicasAssignments.values().iterator().next().size());

    TopicData topicData =
        toTopicData(
            Topic.create(
                clusterId,
                topicName,
                /* partitions= */ emptyList(),
                assumedReplicationFactor,
                /* isInternal= */ false,
                /* authorizedOperations= */ emptySet()));

    boolean validateOnly = request.getValidateOnly().orElse(false);
    TopicManager topicManager = topicManagerProvider.get();

    CompletableFuture<CreateTopicResponse> response =
        topicManager
            .createTopic2(
                clusterId,
                topicName,
                requestPartitionsCount,
                requestReplicationFactor,
                replicasAssignments,
                configs,
                validateOnly)
            .thenApply(topic -> CreateTopicResponse.create(toTopicData(topic)));

    // The response status will differ depending on whether a topic has actually been created.
    Response.Status responseStatus = validateOnly ? Status.OK : Status.CREATED;
    AsyncResponseBuilder.from(
            Response.status(responseStatus).location(URI.create(topicData.getMetadata().getSelf())))
        .entity(response)
        .asyncResume(asyncResponse);
  }

  @DELETE
  @Path("/{topicName}")
  @Produces(MediaType.APPLICATION_JSON)
  @PerformanceMetric("v3.topics.delete")
  @ResourceName("api.v3.topics.delete")
  public void deleteTopic(
      @Suspended AsyncResponse asyncResponse,
      @PathParam("clusterId") String clusterId,
      @PathParam("topicName") String topicName) {
    CompletableFuture<Void> response = topicManagerProvider.get().deleteTopic(clusterId, topicName);

    AsyncResponseBuilder.from(Response.status(Status.NO_CONTENT))
        .entity(response)
        .asyncResume(asyncResponse);
  }

  private TopicData toTopicData(Topic topic) {
    return TopicData.fromTopic(topic)
        .setMetadata(
            Resource.Metadata.builder()
                .setSelf(
                    urlFactory.create(
                        "v3", "clusters", topic.getClusterId(), "topics", topic.getName()))
                .setResourceName(
                    crnFactory.create("kafka", topic.getClusterId(), "topic", topic.getName()))
                .build())
        .setPartitions(
            Resource.Relationship.create(
                urlFactory.create(
                    "v3",
                    "clusters",
                    topic.getClusterId(),
                    "topics",
                    topic.getName(),
                    "partitions")))
        .setConfigs(
            Resource.Relationship.create(
                urlFactory.create(
                    "v3", "clusters", topic.getClusterId(), "topics", topic.getName(), "configs")))
        .setPartitionReassignments(  //TODO remove in cloud
            Resource.Relationship.create(
                urlFactory.create(
                    "v3",
                    "clusters",
                    topic.getClusterId(),
                    "topics",
                    topic.getName(),
                    "partitions",
                    "-",
                    "reassignment")))
        .build();
  }
}
