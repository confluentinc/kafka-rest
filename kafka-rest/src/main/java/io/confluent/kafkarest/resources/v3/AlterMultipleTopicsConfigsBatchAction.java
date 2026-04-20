/*
 * Copyright 2026 Confluent Inc.
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
import io.confluent.kafkarest.controllers.TopicConfigManager;
import io.confluent.kafkarest.entities.v3.AlterMultipleTopicsConfigsBatchRequest;
import io.confluent.kafkarest.entities.v3.AlterMultipleTopicsConfigsBatchResponse;
import io.confluent.kafkarest.entities.v3.AlterMultipleTopicsConfigsBatchResponse.FailureEntry;
import io.confluent.kafkarest.extension.ResourceAccesslistFeature.ResourceName;
import io.confluent.kafkarest.response.StreamingResponse;
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
import java.util.List;
import java.util.concurrent.CompletionException;
import java.util.stream.Collectors;

@Path("/v3/clusters/{clusterId}/topics/-/configs:alter")
@ResourceName("api.v3.topic-configs.*")
public final class AlterMultipleTopicsConfigsBatchAction {

  private final Provider<TopicConfigManager> topicConfigManager;

  @Inject
  public AlterMultipleTopicsConfigsBatchAction(Provider<TopicConfigManager> topicConfigManager) {
    this.topicConfigManager = requireNonNull(topicConfigManager);
  }

  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @PerformanceMetric("v3.topics.configs.alter-multi")
  @ResourceName("api.v3.topic-configs.alter-multi")
  public void alterMultipleTopicsConfigsBatch(
      @Suspended AsyncResponse asyncResponse,
      @PathParam("clusterId") String clusterId,
      @Valid AlterMultipleTopicsConfigsBatchRequest request) {
    if (request == null) {
      throw Errors.invalidPayloadException(Errors.NULL_PAYLOAD_ERROR_MESSAGE);
    }

    boolean validateOnly = request.getValue().getValidateOnly().orElse(false);
    topicConfigManager
        .get()
        .alterMultipleTopicsConfigs(
            clusterId, request.getValue().toAlterConfigCommandsByTopic(), validateOnly)
        .whenComplete(
            (failures, ex) -> {
              if (ex != null) {
                // Pre-validation error (e.g. cluster not found, config name not found) —
                // let the JAX-RS ExceptionMapper translate it to the appropriate HTTP status.
                asyncResponse.resume(
                    ex instanceof CompletionException ? ex.getCause() : ex);
              } else if (failures.isEmpty()) {
                asyncResponse.resume(Response.noContent().build());
              } else {
                List<FailureEntry> entries =
                    failures.entrySet().stream()
                        .map(e -> toFailureEntry(e.getKey(), e.getValue()))
                        .collect(Collectors.toList());
                asyncResponse.resume(
                    Response.status(207, "Multi-Status")
                        .entity(AlterMultipleTopicsConfigsBatchResponse.create(entries))
                        .build());
              }
            });
  }

  private static FailureEntry toFailureEntry(String topicName, Throwable cause) {
    io.confluent.kafkarest.exceptions.v3.ErrorResponse errorResponse =
        StreamingResponse.toErrorResponse(cause);
    return FailureEntry.create(topicName, errorResponse.getErrorCode(), errorResponse.getMessage());
  }
}
