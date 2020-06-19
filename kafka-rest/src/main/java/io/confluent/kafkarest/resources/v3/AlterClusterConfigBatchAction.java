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

import io.confluent.kafkarest.controllers.ClusterConfigManager;
import io.confluent.kafkarest.entities.ClusterConfig;
import io.confluent.kafkarest.entities.v3.AlterClusterConfigBatchRequest;
import io.confluent.kafkarest.resources.AsyncResponses.AsyncResponseBuilder;
import java.util.concurrent.CompletableFuture;
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
import javax.ws.rs.core.Response.Status;

@Path("/v3/clusters/{clusterId}/{config_type}-configs:alter")
public final class AlterClusterConfigBatchAction {

  private final Provider<ClusterConfigManager> clusterConfigManager;

  @Inject
  public AlterClusterConfigBatchAction(Provider<ClusterConfigManager> clusterConfigManager) {
    this.clusterConfigManager = requireNonNull(clusterConfigManager);
  }

  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public void alterClusterConfigBatch(
      @Suspended AsyncResponse asyncResponse,
      @PathParam("clusterId") String clusterId,
      @PathParam("config_type") ClusterConfig.Type configType,
      @Valid AlterClusterConfigBatchRequest request
  ) {
    CompletableFuture<Void> response =
        clusterConfigManager.get()
            .alterClusterConfigs(clusterId, configType, request.getValue().toAlterConfigCommands());

    AsyncResponseBuilder.from(Response.status(Status.NO_CONTENT))
        .entity(response)
        .asyncResume(asyncResponse);
  }
}
