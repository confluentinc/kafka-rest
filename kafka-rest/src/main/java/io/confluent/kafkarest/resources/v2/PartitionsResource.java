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
import io.confluent.kafkarest.controllers.PartitionManager;
import io.confluent.kafkarest.entities.v2.GetPartitionResponse;
import io.confluent.kafkarest.entities.v2.TopicPartitionOffsetResponse;
import io.confluent.kafkarest.extension.ResourceAccesslistFeature.ResourceName;
import io.confluent.kafkarest.resources.AsyncResponses;
import io.confluent.rest.annotations.PerformanceMetric;
import jakarta.inject.Inject;
import jakarta.inject.Provider;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.container.AsyncResponse;
import jakarta.ws.rs.container.Suspended;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

@Path("/topics/{topic}/partitions")
@Consumes({Versions.KAFKA_V2_JSON})
@Produces({Versions.KAFKA_V2_JSON})
@ResourceName("api.v2.partitions.*")
public final class PartitionsResource {

  private final Provider<PartitionManager> partitionManager;

  @Inject
  public PartitionsResource(Provider<PartitionManager> partitionManager) {
    this.partitionManager = requireNonNull(partitionManager);
  }

  @GET
  @PerformanceMetric("partitions.list+v2")
  @ResourceName("api.v2.partitions.list")
  public void list(@Suspended AsyncResponse asyncResponse, @PathParam("topic") String topic) {
    CompletableFuture<List<GetPartitionResponse>> response =
        partitionManager
            .get()
            .listLocalPartitions(topic)
            .thenApply(
                partitions ->
                    partitions.stream()
                        .map(GetPartitionResponse::fromPartition)
                        .collect(Collectors.toList()));

    AsyncResponses.asyncResume(asyncResponse, response);
  }

  @GET
  @Path("/{partition}")
  @PerformanceMetric("partition.get+v2")
  @ResourceName("api.v2.partitions.get")
  public void getPartition(
      @Suspended AsyncResponse asyncResponse,
      @PathParam("topic") String topic,
      @PathParam("partition") int partitionId) {
    CompletableFuture<GetPartitionResponse> response =
        partitionManager
            .get()
            .getLocalPartition(topic, partitionId)
            .thenApply(partition -> partition.orElseThrow(Errors::partitionNotFoundException))
            .thenApply(GetPartitionResponse::fromPartition);

    AsyncResponses.asyncResume(asyncResponse, response);
  }

  /**
   * Returns a summary with beginning and end offsets for the given {@code topic} and {@code
   * partition}.
   *
   * @throws io.confluent.rest.exceptions.RestNotFoundException if either {@code topic} or {@code
   *     partition} don't exist.
   */
  @GET
  @Path("/{partition}/offsets")
  @ResourceName("api.v2.partitions.get-offsets")
  public void getOffsets(
      @Suspended AsyncResponse asyncResponse,
      @PathParam("topic") String topic,
      @PathParam("partition") int partitionId) {
    CompletableFuture<TopicPartitionOffsetResponse> response =
        partitionManager
            .get()
            .getLocalPartition(topic, partitionId)
            .thenApply(partition -> partition.orElseThrow(Errors::partitionNotFoundException))
            .thenApply(
                partition ->
                    new TopicPartitionOffsetResponse(
                        partition.getEarliestOffset(), partition.getLatestOffset()));

    AsyncResponses.asyncResume(asyncResponse, response);
  }
}
