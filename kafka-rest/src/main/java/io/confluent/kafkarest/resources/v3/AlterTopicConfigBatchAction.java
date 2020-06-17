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

import io.confluent.kafkarest.controllers.TopicConfigManager;
import io.confluent.kafkarest.entities.v3.AlterTopicConfigBatchRequest;
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

@Path("/v3/clusters/{clusterId}/topics/{topicName}/configs:alter")
public final class AlterTopicConfigBatchAction {

  private final Provider<TopicConfigManager> topicConfigManager;

  @Inject
  public AlterTopicConfigBatchAction(Provider<TopicConfigManager> topicConfigManager) {
    this.topicConfigManager = requireNonNull(topicConfigManager);
  }

  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public void alterTopicConfigBatch(
      @Suspended AsyncResponse asyncResponse,
      @PathParam("clusterId") String clusterId,
      @PathParam("topicName") String topicName,
      @Valid AlterTopicConfigBatchRequest request
  ) {
    CompletableFuture<Void> response =
        topicConfigManager.get()
            .alterTopicConfigs(clusterId, topicName, request.getValue().toAlterConfigCommands());

    AsyncResponseBuilder.from(Response.status(Status.NO_CONTENT))
        .entity(response)
        .asyncResume(asyncResponse);
  }
}
