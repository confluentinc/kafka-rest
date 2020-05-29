package io.confluent.kafkarest.resources.v3;

import static java.util.Objects.requireNonNull;

import io.confluent.kafkarest.Versions;
import io.confluent.kafkarest.controllers.ReassignmentManager;
import io.confluent.kafkarest.entities.Reassignment;
import io.confluent.kafkarest.entities.v3.ClusterData;
import io.confluent.kafkarest.entities.v3.GetReassignmentResponse;
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
import javax.inject.Inject;
import javax.inject.Provider;
import javax.ws.rs.GET;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;

@Path("/v3/clusters/{clusterId}/topics/{topicName}/partitions/{partitionId}/reassignments")
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
  @Produces(Versions.JSON_API)
  public void getReassignment(
      @Suspended AsyncResponse asyncResponse,
      @PathParam("clusterId") String clusterId,
      @PathParam("topicName") String topicName,
      @PathParam("partitionId") Integer partitionId) {
    CompletableFuture<GetReassignmentResponse> response =
        reassignmentManager.get().getReassignment(clusterId, topicName, partitionId)
            .thenApply(reassignment -> reassignment.orElseThrow(NotFoundException::new))
            .thenApply(
                reassignment -> new GetReassignmentResponse(
                    ReassignmentData.create(crnFactory, urlFactory, reassignment)));

    AsyncResponses.asyncResume(asyncResponse, response);
  }

}
