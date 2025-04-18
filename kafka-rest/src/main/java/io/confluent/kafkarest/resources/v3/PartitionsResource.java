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

import io.confluent.kafkarest.controllers.PartitionManager;
import io.confluent.kafkarest.entities.Partition;
import io.confluent.kafkarest.entities.v3.GetPartitionResponse;
import io.confluent.kafkarest.entities.v3.ListPartitionsResponse;
import io.confluent.kafkarest.entities.v3.PartitionData;
import io.confluent.kafkarest.entities.v3.PartitionDataList;
import io.confluent.kafkarest.entities.v3.Resource;
import io.confluent.kafkarest.entities.v3.ResourceCollection;
import io.confluent.kafkarest.extension.ResourceAccesslistFeature.ResourceName;
import io.confluent.kafkarest.resources.AsyncResponses;
import io.confluent.kafkarest.response.CrnFactory;
import io.confluent.kafkarest.response.UrlFactory;
import io.confluent.rest.annotations.PerformanceMetric;
import jakarta.inject.Inject;
import jakarta.inject.Provider;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.NotFoundException;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.container.AsyncResponse;
import jakarta.ws.rs.container.Suspended;
import jakarta.ws.rs.core.MediaType;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

@Path("/v3/clusters/{clusterId}/topics/{topicName}/partitions")
@ResourceName("api.v3.partitions.*")
public final class PartitionsResource {

  private final Provider<PartitionManager> partitionManager;
  private final CrnFactory crnFactory;
  private final UrlFactory urlFactory;

  @Inject
  public PartitionsResource(
      Provider<PartitionManager> partitionManager, CrnFactory crnFactory, UrlFactory urlFactory) {
    this.partitionManager = requireNonNull(partitionManager);
    this.crnFactory = requireNonNull(crnFactory);
    this.urlFactory = requireNonNull(urlFactory);
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @PerformanceMetric("v3.partitions.list")
  @ResourceName("api.v3.partitions.list")
  public void listPartitions(
      @Suspended AsyncResponse asyncResponse,
      @PathParam("clusterId") String clusterId,
      @PathParam("topicName") String topicName) {
    CompletableFuture<ListPartitionsResponse> response =
        partitionManager
            .get()
            .listPartitions(clusterId, topicName)
            .thenApply(
                partitions ->
                    ListPartitionsResponse.create(
                        PartitionDataList.builder()
                            .setMetadata(
                                ResourceCollection.Metadata.builder()
                                    .setSelf(
                                        urlFactory.create(
                                            "v3",
                                            "clusters",
                                            clusterId,
                                            "topics",
                                            topicName,
                                            "partitions"))
                                    .build())
                            .setData(
                                partitions.stream()
                                    .map(this::toPartitionData)
                                    .collect(Collectors.toList()))
                            .build()));

    AsyncResponses.asyncResume(asyncResponse, response);
  }

  @GET
  @Path("/{partitionId}")
  @Produces(MediaType.APPLICATION_JSON)
  @PerformanceMetric("v3.partitions.get")
  @ResourceName("api.v3.partitions.get")
  public void getPartition(
      @Suspended AsyncResponse asyncResponse,
      @PathParam("clusterId") String clusterId,
      @PathParam("topicName") String topicName,
      @PathParam("partitionId") Integer partitionId) {
    CompletableFuture<GetPartitionResponse> response =
        partitionManager
            .get()
            .getPartition(clusterId, topicName, partitionId)
            .thenApply(partition -> partition.orElseThrow(NotFoundException::new))
            .thenApply(partition -> GetPartitionResponse.create(toPartitionData(partition)));

    AsyncResponses.asyncResume(asyncResponse, response);
  }

  private PartitionData toPartitionData(Partition partition) {
    return toPartitionData(crnFactory, urlFactory, partition);
  }

  static PartitionData toPartitionData(
      CrnFactory crnFactory, UrlFactory urlFactory, Partition partition) {
    PartitionData.Builder partitionData =
        PartitionData.fromPartition(partition)
            .setMetadata(
                Resource.Metadata.builder()
                    .setSelf(
                        urlFactory.create(
                            "v3",
                            "clusters",
                            partition.getClusterId(),
                            "topics",
                            partition.getTopicName(),
                            "partitions",
                            Integer.toString(partition.getPartitionId())))
                    .setResourceName(
                        crnFactory.create(
                            "kafka",
                            partition.getClusterId(),
                            "topic",
                            partition.getTopicName(),
                            "partition",
                            Integer.toString(partition.getPartitionId())))
                    .build())
            .setReplicas(
                Resource.Relationship.create(
                    urlFactory.create(
                        "v3",
                        "clusters",
                        partition.getClusterId(),
                        "topics",
                        partition.getTopicName(),
                        "partitions",
                        Integer.toString(partition.getPartitionId()),
                        "replicas")))
            .setReassignment(
                Resource.Relationship.create(
                    urlFactory.create(
                        "v3",
                        "clusters",
                        partition.getClusterId(),
                        "topics",
                        partition.getTopicName(),
                        "partitions",
                        Integer.toString(partition.getPartitionId()),
                        "reassignment")));

    partition
        .getLeader()
        .map(
            replica ->
                Resource.Relationship.create(
                    urlFactory.create(
                        "v3",
                        "clusters",
                        partition.getClusterId(),
                        "topics",
                        partition.getTopicName(),
                        "partitions",
                        Integer.toString(partition.getPartitionId()),
                        "replicas",
                        Integer.toString(replica.getBrokerId()))))
        .ifPresent(partitionData::setLeader);

    return partitionData.build();
  }
}
