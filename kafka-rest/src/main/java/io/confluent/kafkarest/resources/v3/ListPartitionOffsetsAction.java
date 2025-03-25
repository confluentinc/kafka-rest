/*
 * Copyright 2025 Confluent Inc.
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

import io.confluent.kafkarest.Errors;
import io.confluent.kafkarest.controllers.PartitionManager;
import io.confluent.kafkarest.entities.Partition;
import io.confluent.kafkarest.entities.v3.ListPartitionOffsetsResponse;
import io.confluent.kafkarest.entities.v3.PartitionWithOffsetsData;
import io.confluent.kafkarest.entities.v3.Resource;
import io.confluent.kafkarest.extension.ResourceAccesslistFeature.ResourceName;
import io.confluent.kafkarest.resources.AsyncResponses;
import io.confluent.kafkarest.response.CrnFactory;
import io.confluent.kafkarest.response.UrlFactory;
import io.confluent.rest.annotations.PerformanceMetric;
import jakarta.inject.Inject;
import jakarta.inject.Provider;
import jakarta.ws.rs.BadRequestException;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.container.AsyncResponse;
import jakarta.ws.rs.container.Suspended;
import jakarta.ws.rs.core.MediaType;
import java.util.concurrent.CompletableFuture;

@Path("/v3/clusters/{clusterId}/topics/{topicName}/partitions/{partitionId}/offset")
@ResourceName("api.v3.partition-offsets.*")
public class ListPartitionOffsetsAction {

  private final Provider<PartitionManager> partitionManager;

  private final CrnFactory crnFactory;
  private final UrlFactory urlFactory;

  @Inject
  public ListPartitionOffsetsAction(
      Provider<PartitionManager> partitionManager, CrnFactory crnFactory, UrlFactory urlFactory) {
    this.partitionManager = requireNonNull(partitionManager);
    this.crnFactory = requireNonNull(crnFactory);
    this.urlFactory = requireNonNull(urlFactory);
  }

  /**
   * TODO: If additional offset types are implemented, create an enum and update the API spec to
   * reflect all the types, including the default value.
   */
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @PerformanceMetric("v3.partitions.list.offsets")
  @ResourceName("api.v3.partitions.list.offsets")
  public void listPartitionOffsets(
      @Suspended AsyncResponse asyncResponse,
      @PathParam("clusterId") String clusterId,
      @PathParam("topicName") String topicName,
      @PathParam("partitionId") Integer partitionId,
      @QueryParam("offset_type") @DefaultValue("earliest_and_latest") String offsetType) {

    if (offsetType.equals("earliest_and_latest")) {
      CompletableFuture<ListPartitionOffsetsResponse> response =
          partitionManager
              .get()
              .getPartition(clusterId, topicName, partitionId)
              .thenApply(partition -> partition.orElseThrow(Errors::partitionNotFoundException))
              .thenApply(
                  partition ->
                      ListPartitionOffsetsResponse.create(
                          toPartitionWithOffsetsData(partition, offsetType)));

      AsyncResponses.asyncResume(asyncResponse, response);
    } else {
      throw new BadRequestException("offset_type:" + offsetType + " is invalid");
    }
  }

  private PartitionWithOffsetsData toPartitionWithOffsetsData(
      Partition partition, String offsetType) {
    return toPartitionWithOffsetsData(crnFactory, urlFactory, partition, offsetType);
  }

  static PartitionWithOffsetsData toPartitionWithOffsetsData(
      CrnFactory crnFactory, UrlFactory urlFactory, Partition partition, String offsetType) {
    PartitionWithOffsetsData.Builder partitionWithOffsetsData =
        PartitionWithOffsetsData.fromPartition(partition)
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
                            Integer.toString(partition.getPartitionId()),
                            "offset"))
                    .setResourceName(
                        crnFactory.create(
                            "kafka",
                            partition.getClusterId(),
                            "topic",
                            partition.getTopicName(),
                            "partition",
                            Integer.toString(partition.getPartitionId()),
                            "offset_type",
                            offsetType))
                    .build());
    return partitionWithOffsetsData.build();
  }
}
