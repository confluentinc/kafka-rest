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

import java.util.Collection;
import java.util.List;
import java.util.Vector;

import javax.validation.Valid;
import javax.ws.rs.*;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;

import io.confluent.kafkarest.*;
import io.confluent.kafkarest.entities.*;
import io.confluent.rest.annotations.PerformanceMetric;

@Path("/topics")
@Produces({Versions.KAFKA_V1_JSON_WEIGHTED, Versions.KAFKA_DEFAULT_JSON_WEIGHTED,
           Versions.JSON_WEIGHTED})
@Consumes({Versions.KAFKA_V1_JSON, Versions.KAFKA_DEFAULT_JSON, Versions.JSON,
           Versions.GENERIC_REQUEST})
public class TopicsResource {

  private final Context ctx;

  public TopicsResource(Context ctx) {
    this.ctx = ctx;
  }

  @GET
  @PerformanceMetric("topics.list")
  public Collection<String> list() {
    return ctx.getMetadataObserver().getTopicNames();
  }

  @GET
  @Path("/{topic}")
  @PerformanceMetric("topic.get")
  public Topic getTopic(@PathParam("topic") String topicName) {
    Topic topic = ctx.getMetadataObserver().getTopic(topicName);
    if (topic == null) {
      throw Errors.topicNotFoundException();
    }
    return topic;
  }

  @GET
  @Path("/{topic}/partition/{partition}/messages")
  @PerformanceMetric("topic.consume-binary")
  @Produces({Versions.KAFKA_V1_JSON_BINARY_WEIGHTED,
             Versions.KAFKA_V1_JSON_WEIGHTED,
             Versions.KAFKA_DEFAULT_JSON_WEIGHTED,
             Versions.JSON_WEIGHTED,
             Versions.ANYTHING})
  public void consumeBinary(final @Suspended AsyncResponse asyncResponse,
                            final @PathParam("topic") String topicName,
                            final @PathParam("partition") int partitionId,
                            final @QueryParam("offset") long offset,
                            final @QueryParam("count") @DefaultValue("1") long count) {

    consume(asyncResponse, topicName, partitionId, offset, count, EmbeddedFormat.BINARY);
  }

  @GET
  @Path("/{topic}/partition/{partition}/messages")
  @PerformanceMetric("topic.consume-avro")
  @Produces({Versions.KAFKA_V1_JSON_AVRO_WEIGHTED})
  public void consumeAvro(final @Suspended AsyncResponse asyncResponse,
                            final @PathParam("topic") String topicName,
                            final @PathParam("partition") int partitionId,
                            final @QueryParam("offset") long offset,
                            final @QueryParam("count") @DefaultValue("1") long count) {

    consume(asyncResponse, topicName, partitionId, offset, count, EmbeddedFormat.AVRO);
  }

  @POST
  @Path("/{topic}")
  @PerformanceMetric("topic.produce-binary")
  @Consumes({Versions.KAFKA_V1_JSON_BINARY, Versions.KAFKA_V1_JSON_BINARY, Versions.KAFKA_V1_JSON,
             Versions.KAFKA_DEFAULT_JSON, Versions.JSON, Versions.GENERIC_REQUEST})
  public void produceBinary(final @Suspended AsyncResponse asyncResponse,
                            @PathParam("topic") String topicName,
                            @Valid TopicProduceRequest<BinaryTopicProduceRecord> request) {
    produce(asyncResponse, topicName, EmbeddedFormat.BINARY, request);
  }

  @POST
  @Path("/{topic}")
  @PerformanceMetric("topic.produce-avro")
  @Consumes({Versions.KAFKA_V1_JSON_AVRO})
  public void produceAvro(final @Suspended AsyncResponse asyncResponse,
                          @PathParam("topic") String topicName,
                          @Valid TopicProduceRequest<AvroTopicProduceRecord> request) {
    // Validations we can't do generically since they depend on the data format -- schemas need to
    // be available if there are any non-null entries
    boolean hasKeys = false, hasValues = false;
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
      final TopicProduceRequest<R> request) {
    ctx.getProducerPool().produce(
        topicName, null, format,
        request,
        request.getRecords(),
        new ProducerPool.ProduceRequestCallback() {
          public void onCompletion(Integer keySchemaId, Integer valueSchemaId,
                                   List<RecordMetadataOrException> results) {
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
                                                null, null));
              }
            }
            response.setOffsets(offsets);
            response.setKeySchemaId(keySchemaId);
            response.setValueSchemaId(valueSchemaId);
            asyncResponse.resume(response);
          }
        }
    );
  }

  private <ClientK, ClientV> void consume(
      final @Suspended AsyncResponse asyncResponse,
      final String topicName,
      final int partitionId,
      final long offset,
      final long count,
      final EmbeddedFormat embeddedFormat) {

    ctx.getSimpleConsumerObserver().consume(
        topicName, partitionId, offset, count, embeddedFormat,
        new ConsumerManager.ReadCallback<ClientK, ClientV>() {
          @Override
          public void onCompletion(List<? extends ConsumerRecord<ClientK, ClientV>> records,
                                   Exception e) {
            if (e != null) {
              asyncResponse.resume(e);
            } else {
              asyncResponse.resume(records);
            }
          }
        });
  }
}
