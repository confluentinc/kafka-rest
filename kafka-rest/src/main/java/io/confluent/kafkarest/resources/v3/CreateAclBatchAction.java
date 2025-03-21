/*
 * Copyright 2022 Confluent Inc.
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
import io.confluent.kafkarest.controllers.AclManager;
import io.confluent.kafkarest.entities.Acl;
import io.confluent.kafkarest.entities.v3.CreateAclBatchRequest;
import io.confluent.kafkarest.extension.ResourceAccesslistFeature.ResourceName;
import io.confluent.kafkarest.resources.AsyncResponses;
import io.confluent.rest.annotations.PerformanceMetric;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Provider;
import javax.validation.Valid;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Path("/v3/clusters/{clusterId}/acls:batch")
@ResourceName("api.v3.acls.*")
public final class CreateAclBatchAction {

  private final Provider<AclManager> aclManager;

  @Inject
  public CreateAclBatchAction(Provider<AclManager> aclManager) {
    this.aclManager = requireNonNull(aclManager);
  }

  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @PerformanceMetric("v3.acls.batch-create")
  @ResourceName("api.v3.acls.batch-create")
  public void createAcls(
      @Suspended AsyncResponse asyncResponse,
      @PathParam("clusterId") String clusterId,
      @Valid CreateAclBatchRequest request) {

    if (request == null) {
      throw Errors.invalidPayloadException(Errors.NULL_PAYLOAD_ERROR_MESSAGE);
    }

    CompletableFuture<Void> response =
        aclManager
            .get()
            .validateAclCreateParameters(request.getValue().getData().asList())
            .createAcls(
                clusterId,
                request.getValue().getData().stream()
                    .map(
                        aclEntry ->
                            Acl.builder()
                                .setClusterId(clusterId)
                                .setResourceType(aclEntry.getResourceType())
                                .setResourceName(aclEntry.getResourceName())
                                .setPatternType(aclEntry.getPatternType())
                                .setPrincipal(aclEntry.getPrincipal())
                                .setPermission(aclEntry.getPermission())
                                .setHost(aclEntry.getHost())
                                .setOperation(aclEntry.getOperation())
                                .build())
                    .collect(Collectors.toList()));

    AsyncResponses.AsyncResponseBuilder.from(Response.status(Response.Status.NO_CONTENT))
        .entity(response)
        .asyncResume(asyncResponse);
  }
}
