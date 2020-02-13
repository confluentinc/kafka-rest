/*
 * Copyright 2018 Confluent Inc.
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

import com.fasterxml.jackson.databind.JsonNode;
import io.confluent.kafkarest.Errors;
import io.confluent.kafkarest.KafkaRestContext;
import io.confluent.kafkarest.ProducerPool;
import io.confluent.kafkarest.RecordMetadataOrException;
import io.confluent.kafkarest.Utils;
import io.confluent.kafkarest.Versions;
import io.confluent.kafkarest.entities.EmbeddedFormat;
import io.confluent.kafkarest.entities.PartitionOffset;
import io.confluent.kafkarest.entities.ProduceRecord;
import io.confluent.kafkarest.entities.ProduceRequest;
import io.confluent.kafkarest.entities.ProduceResponse;
import io.confluent.kafkarest.entities.Topic;
import io.confluent.kafkarest.entities.v2.BinaryTopicProduceRequest;
import io.confluent.kafkarest.entities.v2.JsonTopicProduceRequest;
import io.confluent.kafkarest.entities.v2.SchemaTopicProduceRequest;
import io.confluent.rest.annotations.PerformanceMetric;
import java.util.Collection;
import java.util.List;
import java.util.Vector;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/topics")
@Produces({Versions.KAFKA_V2_JSON_WEIGHTED})
@Consumes({Versions.KAFKA_V2_JSON})
public class TopicsResource {

  private static final Logger log = LoggerFactory.getLogger(
      TopicsResource.class);

  private final KafkaRestContext ctx;

  public TopicsResource(KafkaRestContext ctx) {
    this.ctx = ctx;
  }

  @GET
  @PerformanceMetric("topics.list+v2")
  public Collection<String> list() throws Exception {
    return ctx.getAdminClientWrapper().getTopicNames();
  }

  @GET
  @Path("/{topic}")
  @PerformanceMetric("topic.get+v2")
  public Topic getTopic(@PathParam("topic") String topicName) throws Exception {
    Topic topic = ctx.getAdminClientWrapper().getTopic(topicName);
    if (topic == null) {
      throw Errors.topicNotFoundException();
    }
    return topic;
  }

  @POST
  @Path("/{topic}")
  @PerformanceMetric("topic.produce-binary+v2")
  @Consumes({Versions.KAFKA_V2_JSON_BINARY, Versions.KAFKA_V2_JSON})
  public void produceBinary(
      final @Suspended AsyncResponse asyncResponse,
      @PathParam("topic") String topicName,
      @Valid @NotNull BinaryTopicProduceRequest request
  ) {
    produce(asyncResponse, topicName, EmbeddedFormat.BINARY, request.toProduceRequest());
  }

  @POST
  @Path("/{topic}")
  @PerformanceMetric("topic.produce-json+v2")
  @Consumes({Versions.KAFKA_V2_JSON_JSON})
  public void produceJson(
      final @Suspended AsyncResponse asyncResponse,
      @PathParam("topic") String topicName,
      @Valid @NotNull JsonTopicProduceRequest request
  ) {
    produce(asyncResponse, topicName, EmbeddedFormat.JSON, request.toProduceRequest());
  }

  @POST
  @Path("/{topic}")
  @PerformanceMetric("topic.produce-avro+v2")
  @Consumes({Versions.KAFKA_V2_JSON_AVRO})
  public void produceAvro(
      final @Suspended AsyncResponse asyncResponse,
      @PathParam("topic") String topicName,
      @Valid @NotNull SchemaTopicProduceRequest request
  ) {
    // Validations we can't do generically since they depend on the data format -- schemas need to
    // be available if there are any non-null entries
    produceSchema(asyncResponse, topicName, request.toProduceRequest(), EmbeddedFormat.AVRO);
  }

  @POST
  @Path("/{topic}")
  @PerformanceMetric("topic.produce-jsonschema+v2")
  @Consumes({Versions.KAFKA_V2_JSON_JSON_SCHEMA})
  public void produceJsonSchema(
      final @Suspended AsyncResponse asyncResponse,
      @PathParam("topic") String topicName,
      @Valid @NotNull SchemaTopicProduceRequest request
  ) {
    produceSchema(
        asyncResponse,
        topicName,
        request.toProduceRequest(),
        EmbeddedFormat.JSONSCHEMA);
  }

  @POST
  @Path("/{topic}")
  @PerformanceMetric("topic.produce-protobuf+v2")
  @Consumes({Versions.KAFKA_V2_JSON_PROTOBUF})
  public void produceProtobuf(
      final @Suspended AsyncResponse asyncResponse,
      @PathParam("topic") String topicName,
      @Valid @NotNull SchemaTopicProduceRequest request
  ) {
    // Validations we can't do generically since they depend on the data format -- schemas need to
    // be available if there are any non-null entries
    produceSchema(
        asyncResponse,
        topicName,
        request.toProduceRequest(),
        EmbeddedFormat.PROTOBUF);
  }

  public <K, V> void produce(
      final AsyncResponse asyncResponse,
      final String topicName,
      final EmbeddedFormat format,
      final ProduceRequest<K, V> request
  ) {
    log.trace("Executing topic produce request id={} topic={} format={} request={}",
        asyncResponse, topicName, format, request
    );
    ctx.getProducerPool().produce(
        topicName, null, format,
        request,
        new ProducerPool.ProduceRequestCallback() {
          public void onCompletion(
              Integer keySchemaId, Integer valueSchemaId,
              List<RecordMetadataOrException> results
          ) {
            ProduceResponse response = new ProduceResponse();
            List<PartitionOffset> offsets = new Vector<PartitionOffset>();
            for (RecordMetadataOrException result : results) {
              if (result.getException() != null) {
                int errorCode =
                    Utils.errorCodeFromProducerException(result.getException());
                String errorMessage = result.getException().getMessage();
                offsets.add(new PartitionOffset(null, null, errorCode, errorMessage));
              } else {
                offsets.add(new PartitionOffset(result.getRecordMetadata().partition(),
                                                result.getRecordMetadata().offset(),
                                                null, null
                ));
              }
            }
            response.setOffsets(offsets);
            response.setKeySchemaId(keySchemaId);
            response.setValueSchemaId(valueSchemaId);
            log.trace("Completed topic produce request id={} response={}",
                      asyncResponse, response
            );
            Response.Status requestStatus = Utils.produceRequestStatus(response);
            asyncResponse.resume(Response.status(requestStatus).entity(response).build());
          }
        }
    );
  }

  private void produceSchema(
      AsyncResponse asyncResponse,
      String topicName,
      ProduceRequest<JsonNode, JsonNode> request,
      EmbeddedFormat jsonschema
  ) {
    checkKeySchema(request);
    checkValueSchema(request);
    produce(asyncResponse, topicName, jsonschema, request);
  }

  private static void checkKeySchema(ProduceRequest<JsonNode, ?> request) {
    for (ProduceRecord<JsonNode, ?> record : request.getRecords()) {
      if (record.getKey() == null || record.getKey().isNull()) {
        continue;
      }
      if (request.getKeySchema() != null || request.getKeySchemaId() != null) {
        continue;
      }
      throw Errors.keySchemaMissingException();
    }
  }

  private static void checkValueSchema(ProduceRequest<?, JsonNode> request) {
    for (ProduceRecord<?, JsonNode> record : request.getRecords()) {
      if (record.getValue() == null || record.getValue().isNull()) {
        continue;
      }
      if (request.getValueSchema() != null || request.getValueSchemaId() != null) {
        continue;
      }
      throw Errors.valueSchemaMissingException();
    }
  }
}
