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
import io.confluent.kafkarest.controllers.ReassignmentManager;
import io.confluent.kafkarest.entities.Reassignment;
import io.confluent.kafkarest.entities.v3.ClusterData;
import io.confluent.kafkarest.entities.v3.CollectionLink;
import io.confluent.kafkarest.entities.v3.ListReassignmentsResponse;
import io.confluent.kafkarest.entities.v3.PartitionData;
import io.confluent.kafkarest.entities.v3.ReassignmentData;
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
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;

@Path("/v3/clusters/{clusterId}/topics/-/partitions/-/reassignments")
public final class ListAllReassignmentsAction {

  private final Provider<ReassignmentManager> reassignmentManager;
  private final CrnFactory crnFactory;
  private final UrlFactory urlFactory;

  @Inject
  public ListAllReassignmentsAction(
      Provider<ReassignmentManager> reassignmentManager,
      CrnFactory crnFactory,
      UrlFactory urlFactory) {
    this.reassignmentManager = requireNonNull(reassignmentManager);
    this.crnFactory = requireNonNull(crnFactory);
    this.urlFactory = requireNonNull(urlFactory);
  }

  @GET
  @Produces(Versions.JSON_API)
  public void listReassignments(
      @Suspended AsyncResponse asyncResponse,
      @PathParam("clusterId") String clusterId) {
    CompletableFuture<ListReassignmentsResponse> response =
        reassignmentManager.get().listReassignments(clusterId)
            .thenApply(
                reassignments ->
                    new ListReassignmentsResponse(
                        new CollectionLink(
                            urlFactory.create("v3", "clusters", clusterId, "topics", "-",
                                "partitions", "-", "reassignments"),
                            /* next= */ null),
                        reassignments.stream()
                            .map(this::toReassignmentData)
                            .collect(Collectors.toList())));

    AsyncResponses.asyncResume(asyncResponse, response);
  }

  private ReassignmentData toReassignmentData(Reassignment reassignment) {
    ResourceLink links = new ResourceLink(
        urlFactory.create(
            "v3",
            "clusters",
            reassignment.getClusterId(),
            "topics",
            reassignment.getTopicName(),
            "partitions",
            Integer.toString(reassignment.getPartitionId()),
            "reassignments",
            Integer.toString(reassignment.getPartitionId())));

    Relationship replicas = new Relationship(urlFactory.create(
        "v3",
        "clusters",
        reassignment.getClusterId(),
        "topics",
        reassignment.getTopicName(),
        "partitions",
        Integer.toString(reassignment.getPartitionId()),
        "replicas"));

    return new ReassignmentData(
        crnFactory.create(
            ClusterData.ELEMENT_TYPE,
            reassignment.getClusterId(),
            TopicData.ELEMENT_TYPE,
            reassignment.getTopicName(),
            PartitionData.ELEMENT_TYPE,
            Integer.toString(reassignment.getPartitionId()),
            ReassignmentData.ELEMENT_TYPE,
            Integer.toString(reassignment.getPartitionId())),
        links,
        reassignment.getClusterId(),
        reassignment.getTopicName(),
        reassignment.getPartitionId(),
        reassignment.getReplicas(),
        reassignment.getAddingReplicas(),
        reassignment.getRemovingReplicas(),
        replicas);
  }
}
