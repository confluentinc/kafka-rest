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

import io.confluent.kafkarest.controllers.ReassignmentManager;
import io.confluent.kafkarest.entities.Reassignment;
import io.confluent.kafkarest.entities.v3.GetReassignmentResponse;
import io.confluent.kafkarest.entities.v3.ReassignmentData;
import io.confluent.kafkarest.entities.v3.Resource;
import io.confluent.kafkarest.resources.AsyncResponses;
import io.confluent.kafkarest.response.CrnFactory;
import io.confluent.kafkarest.response.UrlFactory;
import java.util.concurrent.CompletableFuture;
import javax.inject.Inject;
import javax.inject.Provider;
import javax.ws.rs.GET;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.MediaType;

@Path("/v3/clusters/{clusterId}/topics/{topicName}/partitions/{partitionId}/reassignment")
public final class GetReassignmentAction {

  private final Provider<ReassignmentManager> reassignmentManager;
  private final CrnFactory crnFactory;
  private final UrlFactory urlFactory;

  @Inject
  public GetReassignmentAction(
      Provider<ReassignmentManager> reassignmentManager,
      CrnFactory crnFactory,
      UrlFactory urlFactory) {
    this.reassignmentManager = requireNonNull(reassignmentManager);
    this.crnFactory = requireNonNull(crnFactory);
    this.urlFactory = requireNonNull(urlFactory);
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public void getReassignment(
      @Suspended AsyncResponse asyncResponse,
      @PathParam("clusterId") String clusterId,
      @PathParam("topicName") String topicName,
      @PathParam("partitionId") Integer partitionId) {
    CompletableFuture<GetReassignmentResponse> response =
        reassignmentManager.get().getReassignment(clusterId, topicName, partitionId)
            .thenApply(reassignment -> reassignment.orElseThrow(NotFoundException::new))
            .thenApply(
                reassignment -> GetReassignmentResponse.create(toReassignmentData(reassignment)));

    AsyncResponses.asyncResume(asyncResponse, response);
  }

  private ReassignmentData toReassignmentData(Reassignment reassignment) {
    return ReassignmentData.fromReassignment(reassignment)
        .setMetadata(
            Resource.Metadata.builder()
                .setSelf(
                    urlFactory.create(
                        "v3",
                        "clusters",
                        reassignment.getClusterId(),
                        "topics",
                        reassignment.getTopicName(),
                        "partitions",
                        Integer.toString(reassignment.getPartitionId()),
                        "reassignment"))
                .setResourceName(
                    crnFactory.create(
                        "kafka",
                        reassignment.getClusterId(),
                        "topic",
                        reassignment.getTopicName(),
                        "partition",
                        Integer.toString(reassignment.getPartitionId()),
                        "reassignment",
                        null))
                .build())
        .setReplicas(
            Resource.Relationship.create(
                urlFactory.create(
                    "v3",
                    "clusters",
                    reassignment.getClusterId(),
                    "topics",
                    reassignment.getTopicName(),
                    "partitions",
                    Integer.toString(reassignment.getPartitionId()),
                    "replicas")))
        .build();
  }
}
