/*
 * Copyright 2021 Confluent Inc.
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

import io.confluent.kafkarest.Versions;
import io.confluent.kafkarest.controllers.ProduceController;
import io.confluent.kafkarest.controllers.RecordSerializer;
import io.confluent.kafkarest.controllers.SchemaManager;
import io.confluent.kafkarest.entities.EmbeddedFormat;
import io.confluent.kafkarest.entities.v2.ProduceRequest;
import io.confluent.kafkarest.entities.v2.ProduceResponse;
import io.confluent.kafkarest.extension.ResourceBlocklistFeature.ResourceName;
import io.confluent.kafkarest.resources.AsyncResponses.AsyncResponseBuilder;
import io.confluent.rest.annotations.PerformanceMetric;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import javax.inject.Inject;
import javax.inject.Provider;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Response;

@Path("/topics")
@Consumes({
    Versions.KAFKA_V2_JSON_BINARY,
    Versions.KAFKA_V2_JSON_JSON,
    Versions.KAFKA_V2_JSON_AVRO,
    Versions.KAFKA_V2_JSON_PROTOBUF,
    Versions.KAFKA_V2_JSON_JSON_SCHEMA
})
@Produces({Versions.KAFKA_V2_JSON})
@ResourceName("api.v2.produce-to-topic.*")
public final class ProduceToTopicAction extends AbstractProduceAction {

  @Inject
  public ProduceToTopicAction(
      Provider<SchemaManager> schemaManager,
      Provider<RecordSerializer> recordSerializer,
      Provider<ProduceController> produceController) {
    super(schemaManager, recordSerializer, produceController);
  }

  @POST
  @Path("/{topic}")
  @PerformanceMetric("topic.produce-binary+v2")
  @Consumes({Versions.KAFKA_V2_JSON_BINARY_WEIGHTED})
  @ResourceName("api.v2.produce-to-topic.binary")
  public void produceBinary(
      @Suspended AsyncResponse asyncResponse,
      @PathParam("topic") String topicName,
      @Valid @NotNull ProduceRequest request
  ) {
    CompletableFuture<ProduceResponse> response =
        produceWithoutSchema(
            EmbeddedFormat.BINARY, topicName, /* partitionId= */ Optional.empty(), request);

    AsyncResponseBuilder.<ProduceResponse>from(Response.ok())
        .entity(response)
        .status(ProduceResponse::getRequestStatus)
        .asyncResume(asyncResponse);
  }

  @POST
  @Path("/{topic}")
  @PerformanceMetric("topic.produce-json+v2")
  @Consumes({Versions.KAFKA_V2_JSON_JSON_WEIGHTED_LOW})
  @ResourceName("api.v2.produce-to-topic.json")
  public void produceJson(
      @Suspended AsyncResponse asyncResponse,
      @PathParam("topic") String topicName,
      @Valid @NotNull ProduceRequest request
  ) {
    CompletableFuture<ProduceResponse> response =
        produceWithoutSchema(
            EmbeddedFormat.JSON, topicName, /* partitionId= */ Optional.empty(), request);

    AsyncResponseBuilder.<ProduceResponse>from(Response.ok())
        .entity(response)
        .status(ProduceResponse::getRequestStatus)
        .asyncResume(asyncResponse);
  }

  @POST
  @Path("/{topic}")
  @PerformanceMetric("topic.produce-avro+v2")
  @Consumes({Versions.KAFKA_V2_JSON_AVRO_WEIGHTED_LOW})
  @ResourceName("api.v2.produce-to-topic.avro")
  public void produceAvro(
      @Suspended AsyncResponse asyncResponse,
      @PathParam("topic") String topicName,
      @Valid @NotNull ProduceRequest request
  ) {
    CompletableFuture<ProduceResponse> response =
        produceWithSchema(
            EmbeddedFormat.AVRO, topicName, /* partitionId= */ Optional.empty(), request);

    AsyncResponseBuilder.<ProduceResponse>from(Response.ok())
        .entity(response)
        .status(ProduceResponse::getRequestStatus)
        .asyncResume(asyncResponse);
  }

  @POST
  @Path("/{topic}")
  @PerformanceMetric("topic.produce-jsonschema+v2")
  @Consumes({Versions.KAFKA_V2_JSON_JSON_SCHEMA_WEIGHTED_LOW})
  @ResourceName("api.v2.produce-to-topic.json-schema")
  public void produceJsonSchema(
      @Suspended AsyncResponse asyncResponse,
      @PathParam("topic") String topicName,
      @Valid @NotNull ProduceRequest request
  ) {
    CompletableFuture<ProduceResponse> response =
        produceWithSchema(
            EmbeddedFormat.JSONSCHEMA, /* partitionId= */ topicName, Optional.empty(), request);

    AsyncResponseBuilder.<ProduceResponse>from(Response.ok())
        .entity(response)
        .status(ProduceResponse::getRequestStatus)
        .asyncResume(asyncResponse);
  }

  @POST
  @Path("/{topic}")
  @PerformanceMetric("topic.produce-protobuf+v2")
  @Consumes({Versions.KAFKA_V2_JSON_PROTOBUF_WEIGHTED_LOW})
  @ResourceName("api.v2.produce-to-topic.protobuf")
  public void produceProtobuf(
      @Suspended AsyncResponse asyncResponse,
      @PathParam("topic") String topicName,
      @Valid @NotNull ProduceRequest request
  ) {
    CompletableFuture<ProduceResponse> response =
        produceWithSchema(
            EmbeddedFormat.PROTOBUF, topicName, /* partitionId= */ Optional.empty(), request);

    AsyncResponseBuilder.<ProduceResponse>from(Response.ok())
        .entity(response)
        .status(ProduceResponse::getRequestStatus)
        .asyncResume(asyncResponse);
  }
}
