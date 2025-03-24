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

import io.confluent.kafkarest.Errors;
import io.confluent.kafkarest.controllers.ClusterConfigManager;
import io.confluent.kafkarest.entities.ClusterConfig;
import io.confluent.kafkarest.entities.v3.AlterClusterConfigBatchRequest;
import io.confluent.kafkarest.extension.ResourceAccesslistFeature.ResourceName;
import io.confluent.kafkarest.resources.AsyncResponses.AsyncResponseBuilder;
import io.confluent.rest.annotations.PerformanceMetric;
import jakarta.inject.Inject;
import jakarta.inject.Provider;
import jakarta.validation.Valid;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.container.AsyncResponse;
import jakarta.ws.rs.container.Suspended;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.Response.Status;
import java.util.concurrent.CompletableFuture;

@Path("/v3/clusters/{clusterId}/{config_type}-configs:alter")
@ResourceName("api.v3.cluster-configs.*")
public final class AlterClusterConfigBatchAction {

  private final Provider<ClusterConfigManager> clusterConfigManager;

  @Inject
  public AlterClusterConfigBatchAction(Provider<ClusterConfigManager> clusterConfigManager) {
    this.clusterConfigManager = requireNonNull(clusterConfigManager);
  }

  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @PerformanceMetric("v3.clusters.configs.alter")
  @ResourceName("api.v3.cluster-configs.alter")
  public void alterClusterConfigBatch(
      @Suspended AsyncResponse asyncResponse,
      @PathParam("clusterId") String clusterId,
      @PathParam("config_type") ClusterConfig.Type configType,
      @Valid AlterClusterConfigBatchRequest request) {
    if (request == null) {
      throw Errors.invalidPayloadException(Errors.NULL_PAYLOAD_ERROR_MESSAGE);
    }

    CompletableFuture<Void> response =
        clusterConfigManager
            .get()
            .alterClusterConfigs(clusterId, configType, request.getValue().toAlterConfigCommands());

    AsyncResponseBuilder.from(Response.status(Status.NO_CONTENT))
        .entity(response)
        .asyncResume(asyncResponse);
  }
}
