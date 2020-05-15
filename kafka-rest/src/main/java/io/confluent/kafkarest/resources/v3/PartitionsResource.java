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
import io.confluent.kafkarest.controllers.PartitionManager;
import io.confluent.kafkarest.entities.Partition;
import io.confluent.kafkarest.entities.v3.ClusterData;
import io.confluent.kafkarest.entities.v3.CollectionLink;
import io.confluent.kafkarest.entities.v3.GetPartitionResponse;
import io.confluent.kafkarest.entities.v3.ListPartitionsResponse;
import io.confluent.kafkarest.entities.v3.PartitionData;
import io.confluent.kafkarest.entities.v3.Relationship;
import io.confluent.kafkarest.entities.v3.ResourceLink;
import io.confluent.kafkarest.entities.v3.TopicData;
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

@Path("/v3/clusters/{clusterId}/topics/{topicName}/partitions")
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
  @Produces(Versions.JSON_API)
  public void listPartitions(
      @Suspended AsyncResponse asyncResponse,
      @PathParam("clusterId") String clusterId,
      @PathParam("topicName") String topicName) {
    CompletableFuture<ListPartitionsResponse> response =
        partitionManager.get()
            .listPartitions(clusterId, topicName)
            .thenApply(
                partitions ->
                    new ListPartitionsResponse(
                        new CollectionLink(
                            urlFactory.create(
                                "v3", "clusters", clusterId, "topics", topicName, "partitions"),
                            /* next= */ null),
                        partitions.stream()
                            .map(this::toPartitionData)
                            .collect(Collectors.toList())));

    AsyncResponses.asyncResume(asyncResponse, response);
  }

  @GET
  @Path("/{partitionId}")
  @Produces(Versions.JSON_API)
  public void getPartition(
      @Suspended AsyncResponse asyncResponse,
      @PathParam("clusterId") String clusterId,
      @PathParam("topicName") String topicName,
      @PathParam("partitionId") Integer partitionId) {
    CompletableFuture<GetPartitionResponse> response =
        partitionManager.get()
            .getPartition(clusterId, topicName, partitionId)
            .thenApply(partition -> partition.orElseThrow(NotFoundException::new))
            .thenApply(partition -> new GetPartitionResponse(toPartitionData(partition)));

    AsyncResponses.asyncResume(asyncResponse, response);
  }

  private PartitionData toPartitionData(Partition partition) {
    ResourceLink links =
        new ResourceLink(
            urlFactory.create(
                "v3",
                "clusters",
                partition.getClusterId(),
                "topics",
                partition.getTopicName(),
                "partitions",
                Integer.toString(partition.getPartitionId())));
    Relationship leader =
        partition.getLeader()
            .map(
                replica ->
                    new Relationship(
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
            .orElse(null);
    Relationship replicas =
        new Relationship(
            urlFactory.create(
                "v3",
                "clusters",
                partition.getClusterId(),
                "topics",
                partition.getTopicName(),
                "partitions",
                Integer.toString(partition.getPartitionId()),
                "replicas"));

    return new PartitionData(
        crnFactory.create(
            ClusterData.ELEMENT_TYPE,
            partition.getClusterId(),
            TopicData.ELEMENT_TYPE,
            partition.getTopicName(),
            PartitionData.ELEMENT_TYPE,
            Integer.toString(partition.getPartitionId())),
        links,
        partition.getClusterId(),
        partition.getTopicName(),
        partition.getPartitionId(),
        leader,
        replicas);
  }
}
