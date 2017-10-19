/**
 * Copyright 2015 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.kafkarest.resources;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

import io.confluent.kafkarest.KafkaRestContext;
import io.confluent.kafkarest.Errors;
import io.confluent.kafkarest.ProducerPool;
import io.confluent.kafkarest.RecordMetadataOrException;
import io.confluent.kafkarest.Versions;
import io.confluent.kafkarest.entities.AvroTopicProduceRecord;
import io.confluent.kafkarest.entities.BinaryTopicProduceRecord;
import io.confluent.kafkarest.entities.EmbeddedFormat;
import io.confluent.kafkarest.entities.JsonTopicProduceRecord;
import io.confluent.kafkarest.entities.PartitionOffset;
import io.confluent.kafkarest.entities.ProduceResponse;
import io.confluent.kafkarest.entities.Topic;
import io.confluent.kafkarest.entities.TopicProduceRecord;
import io.confluent.kafkarest.entities.TopicProduceRequest;
import io.confluent.rest.annotations.PerformanceMetric;

@Path("/topics")
@Produces({Versions.KAFKA_V1_JSON_WEIGHTED, Versions.KAFKA_DEFAULT_JSON_WEIGHTED,
           Versions.JSON_WEIGHTED, Versions.KAFKA_V2_JSON_WEIGHTED})
@Consumes({Versions.KAFKA_V1_JSON, Versions.KAFKA_DEFAULT_JSON, Versions.JSON,
           Versions.GENERIC_REQUEST, Versions.KAFKA_V2_JSON})
public class TopicsResource {

  private static final Logger log = LoggerFactory.getLogger(TopicsResource.class);

  private final KafkaRestContext ctx;

  public TopicsResource(KafkaRestContext ctx) {
    this.ctx = ctx;
  }

  @GET
  @PerformanceMetric("topics.list")
  public Collection<String> list() {
    return ctx.getAdminClientWrapper().getTopicNames();
  }

  @GET
  @Path("/{topic}")
  @PerformanceMetric("topic.get")
  public Topic getTopic(@PathParam("topic") String topicName) {
    Topic topic = ctx.getAdminClientWrapper().getTopic(topicName);
    if (topic == null) {
      throw Errors.topicNotFoundException();
    }
    return topic;
  }

  @POST
  @Path("/{topic}")
  @PerformanceMetric("topic.produce-binary")
  @Consumes({Versions.KAFKA_V1_JSON_BINARY, Versions.KAFKA_V1_JSON,
             Versions.KAFKA_DEFAULT_JSON, Versions.JSON, Versions.GENERIC_REQUEST,
             Versions.KAFKA_V2_JSON_BINARY, Versions.KAFKA_V2_JSON})
  public void produceBinary(
      final @Suspended AsyncResponse asyncResponse,
      @PathParam("topic") String topicName,
      @Valid @NotNull TopicProduceRequest<BinaryTopicProduceRecord> request
  ) {
    produce(asyncResponse, topicName, EmbeddedFormat.BINARY, request);
  }

  @POST
  @Path("/{topic}")
  @PerformanceMetric("topic.produce-json")
  @Consumes({Versions.KAFKA_V1_JSON_JSON, Versions.KAFKA_V2_JSON_JSON})
  public void produceJson(
      final @Suspended AsyncResponse asyncResponse,
      @PathParam("topic") String topicName,
      @Valid @NotNull TopicProduceRequest<JsonTopicProduceRecord> request
  ) {
    produce(asyncResponse, topicName, EmbeddedFormat.JSON, request);
  }

  @POST
  @Path("/{topic}")
  @PerformanceMetric("topic.produce-avro")
  @Consumes({Versions.KAFKA_V1_JSON_AVRO, Versions.KAFKA_V2_JSON_AVRO})
  public void produceAvro(
      final @Suspended AsyncResponse asyncResponse,
      @PathParam("topic") String topicName,
      @Valid @NotNull TopicProduceRequest<AvroTopicProduceRecord> request
  ) {
    // Validations we can't do generically since they depend on the data format -- schemas need to
    // be available if there are any non-null entries
    boolean hasKeys = false;
    boolean hasValues = false;
    for (AvroTopicProduceRecord rec : request.getRecords()) {
      hasKeys = hasKeys || !rec.getJsonKey().isNull();
      hasValues = hasValues || !rec.getJsonValue().isNull();
    }
    if (hasKeys && request.getKeySchema() == null && request.getKeySchemaId() == null) {
      throw Errors.keySchemaMissingException();
    }
    if (hasValues && request.getValueSchema() == null && request.getValueSchemaId() == null) {
      throw Errors.valueSchemaMissingException();
    }

    produce(asyncResponse, topicName, EmbeddedFormat.AVRO, request);
  }

  public <K, V, R extends TopicProduceRecord<K, V>> void produce(
      final AsyncResponse asyncResponse,
      final String topicName,
      final EmbeddedFormat format,
      final TopicProduceRequest<R> request
  ) {
    log.trace("Executing topic produce request id={} topic={} format={} request={}",
              asyncResponse, topicName, format, request
    );
    ctx.getProducerPool().produce(
        topicName, null, format,
        request,
        request.getRecords(),
        new ProducerPool.ProduceRequestCallback() {
          public void onCompletion(
              Integer keySchemaId, Integer valueSchemaId,
              List<RecordMetadataOrException> results
          ) {
            ProduceResponse response = new ProduceResponse();
            List<PartitionOffset> offsets = new Vector<PartitionOffset>();
            for (RecordMetadataOrException result : results) {
              if (result.getException() != null) {
                int errorCode = Errors.codeFromProducerException(result.getException());
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
            asyncResponse.resume(response);
          }
        }
    );
  }

}
