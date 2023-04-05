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

package io.confluent.kafkarest.resources.v2;

import io.confluent.kafkarest.KafkaRestContext;
import io.confluent.kafkarest.Versions;
import io.confluent.kafkarest.entities.EmbeddedFormat;
import io.confluent.kafkarest.entities.v2.BinaryTopicProduceRequest;
import io.confluent.kafkarest.entities.v2.JsonTopicProduceRequest;
import io.confluent.kafkarest.entities.v2.SchemaTopicProduceRequest;
import io.confluent.rest.annotations.PerformanceMetric;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;

@Path("/topics")
@Consumes({
    Versions.KAFKA_V2_JSON_BINARY,
    Versions.KAFKA_V2_JSON_JSON,
    Versions.KAFKA_V2_JSON_AVRO,
    Versions.KAFKA_V2_JSON_PROTOBUF,
    Versions.KAFKA_V2_JSON_JSON_SCHEMA
})
@Produces({Versions.KAFKA_V2_JSON})
public final class ProduceToTopicAction extends AbstractProduceAction {

  public ProduceToTopicAction(KafkaRestContext ctx) {
    super(ctx);
  }

  @POST
  @Path("/{topic}")
  @PerformanceMetric("topic.produce-binary+v2")
  @Consumes({Versions.KAFKA_V2_JSON_BINARY_WEIGHTED})
  public void produceBinary(
      @Suspended AsyncResponse asyncResponse,
      @PathParam("topic") String topicName,
      @Valid @NotNull BinaryTopicProduceRequest request
  ) {
    produce(
        asyncResponse,
        topicName,
        /* partition= */ null,
        EmbeddedFormat.BINARY,
        request.toProduceRequest());
  }

  @POST
  @Path("/{topic}")
  @PerformanceMetric("topic.produce-json+v2")
  @Consumes({Versions.KAFKA_V2_JSON_JSON_WEIGHTED_LOW})
  public void produceJson(
      @Suspended AsyncResponse asyncResponse,
      @PathParam("topic") String topicName,
      @Valid @NotNull JsonTopicProduceRequest request
  ) {
    produce(
        asyncResponse,
        topicName,
        /* partition= */ null,
        EmbeddedFormat.JSON,
        request.toProduceRequest());
  }

  @POST
  @Path("/{topic}")
  @PerformanceMetric("topic.produce-avro+v2")
  @Consumes({Versions.KAFKA_V2_JSON_AVRO_WEIGHTED_LOW})
  public void produceAvro(
      @Suspended AsyncResponse asyncResponse,
      @PathParam("topic") String topicName,
      @Valid @NotNull SchemaTopicProduceRequest request
  ) {
    produceSchema(
        asyncResponse,
        topicName,
        /* partition= */ null,
        request.toProduceRequest(),
        EmbeddedFormat.AVRO);
  }

  @POST
  @Path("/{topic}")
  @PerformanceMetric("topic.produce-jsonschema+v2")
  @Consumes({Versions.KAFKA_V2_JSON_JSON_SCHEMA_WEIGHTED_LOW})
  public void produceJsonSchema(
      @Suspended AsyncResponse asyncResponse,
      @PathParam("topic") String topicName,
      @Valid @NotNull SchemaTopicProduceRequest request
  ) {
    produceSchema(
        asyncResponse,
        topicName,
        /* partition= */ null,
        request.toProduceRequest(),
        EmbeddedFormat.JSONSCHEMA);
  }

  @POST
  @Path("/{topic}")
  @PerformanceMetric("topic.produce-protobuf+v2")
  @Consumes({Versions.KAFKA_V2_JSON_PROTOBUF_WEIGHTED_LOW})
  public void produceProtobuf(
      @Suspended AsyncResponse asyncResponse,
      @PathParam("topic") String topicName,
      @Valid @NotNull SchemaTopicProduceRequest request
  ) {
    produceSchema(
        asyncResponse,
        topicName,
        /* partition= */ null,
        request.toProduceRequest(),
        EmbeddedFormat.PROTOBUF);
  }
}
