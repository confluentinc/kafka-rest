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

import java.util.List;
import java.util.Vector;

import javax.validation.Valid;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;

import io.confluent.kafkarest.ConsumerManager;
import io.confluent.kafkarest.entities.AbstractConsumerRecord;
import io.confluent.kafkarest.Context;
import io.confluent.kafkarest.Errors;
import io.confluent.kafkarest.ProducerPool;
import io.confluent.kafkarest.RecordMetadataOrException;
import io.confluent.kafkarest.Versions;
import io.confluent.kafkarest.entities.AvroProduceRecord;
import io.confluent.kafkarest.entities.BinaryProduceRecord;
import io.confluent.kafkarest.entities.EmbeddedFormat;
import io.confluent.kafkarest.entities.JsonProduceRecord;
import io.confluent.kafkarest.entities.Partition;
import io.confluent.kafkarest.entities.PartitionOffset;
import io.confluent.kafkarest.entities.PartitionProduceRequest;
import io.confluent.kafkarest.entities.ProduceRecord;
import io.confluent.kafkarest.entities.ProduceResponse;
import io.confluent.rest.annotations.PerformanceMetric;

@Path("/topics/{topic}/partitions")
@Produces({Versions.KAFKA_V1_JSON_BINARY_WEIGHTED_LOW, Versions.KAFKA_V1_JSON_AVRO_WEIGHTED_LOW,
    Versions.KAFKA_V1_JSON_WEIGHTED, Versions.KAFKA_DEFAULT_JSON_WEIGHTED,
    Versions.JSON_WEIGHTED})
@Consumes({Versions.KAFKA_V1_JSON, Versions.KAFKA_DEFAULT_JSON, Versions.JSON,
           Versions.GENERIC_REQUEST})
public class PartitionsResource {

  private final Context ctx;

  public PartitionsResource(Context ctx) {
    this.ctx = ctx;
  }

  @GET
  @PerformanceMetric("partitions.list")
  public List<Partition> list(final @PathParam("topic") String topic) {
    checkTopicExists(topic);
    return ctx.getMetadataObserver().getTopicPartitions(topic);
  }

  @GET
  @Path("/{partition}")
  @PerformanceMetric("partition.get")
  public Partition getPartition(final @PathParam("topic") String topic,
                                @PathParam("partition") int partition) {
    checkTopicExists(topic);
    Partition part = ctx.getMetadataObserver().getTopicPartition(topic, partition);
    if (part == null) {
      throw Errors.partitionNotFoundException();
    }
    return part;
  }


  @GET
  @Path("/{partition}/messages")
  @PerformanceMetric("partition.consume-binary")
  @Produces({Versions.KAFKA_V1_JSON_BINARY_WEIGHTED,
      Versions.KAFKA_V1_JSON_WEIGHTED,
      Versions.KAFKA_DEFAULT_JSON_WEIGHTED,
      Versions.JSON_WEIGHTED})
  public void consumeBinary(final @Suspended AsyncResponse asyncResponse,
                            final @PathParam("topic") String topicName,
                            final @PathParam("partition") int partitionId,
                            final @QueryParam("offset") long offset,
                            final @QueryParam("count") @DefaultValue("1") long count) {

    consume(asyncResponse, topicName, partitionId, offset, count, EmbeddedFormat.BINARY);
  }

  @GET
  @Path("/{partition}/messages")
  @PerformanceMetric("partition.consume-avro")
  @Produces({Versions.KAFKA_V1_JSON_AVRO_WEIGHTED_LOW})
  public void consumeAvro(final @Suspended AsyncResponse asyncResponse,
                          final @PathParam("topic") String topicName,
                          final @PathParam("partition") int partitionId,
                          final @QueryParam("offset") long offset,
                          final @QueryParam("count") @DefaultValue("1") long count) {

    consume(asyncResponse, topicName, partitionId, offset, count, EmbeddedFormat.AVRO);
  }

  @GET
  @Path("/{partition}/messages")
  @PerformanceMetric("partition.consume-json")
  @Produces({Versions.KAFKA_V1_JSON_JSON_WEIGHTED_LOW})
  public void consumeJson(final @Suspended AsyncResponse asyncResponse,
                          final @PathParam("topic") String topicName,
                          final @PathParam("partition") int partitionId,
                          final @QueryParam("offset") long offset,
                          final @QueryParam("count") @DefaultValue("1") long count) {

    consume(asyncResponse, topicName, partitionId, offset, count, EmbeddedFormat.JSON);
  }

  @POST
  @Path("/{partition}")
  @PerformanceMetric("partition.produce-binary")
  @Consumes({Versions.KAFKA_V1_JSON_BINARY, Versions.KAFKA_V1_JSON,
             Versions.KAFKA_DEFAULT_JSON, Versions.JSON, Versions.GENERIC_REQUEST})
  public void produceBinary(final @Suspended AsyncResponse asyncResponse,
                            final @PathParam("topic") String topic,
                            final @PathParam("partition") int partition,
                            @Valid PartitionProduceRequest<BinaryProduceRecord> request) {
    produce(asyncResponse, topic, partition, EmbeddedFormat.BINARY, request);
  }

  @POST
  @Path("/{partition}")
  @PerformanceMetric("partition.produce-json")
  @Consumes({Versions.KAFKA_V1_JSON_JSON})
  public void produceJson(final @Suspended AsyncResponse asyncResponse,
                          final @PathParam("topic") String topic,
                          final @PathParam("partition") int partition,
                          @Valid PartitionProduceRequest<JsonProduceRecord> request) {
    produce(asyncResponse, topic, partition, EmbeddedFormat.JSON, request);
  }

  @POST
  @Path("/{partition}")
  @PerformanceMetric("partition.produce-avro")
  @Consumes({Versions.KAFKA_V1_JSON_AVRO})
  public void produceAvro(final @Suspended AsyncResponse asyncResponse,
                          final @PathParam("topic") String topic,
                          final @PathParam("partition") int partition,
                          @Valid PartitionProduceRequest<AvroProduceRecord> request) {
    // Validations we can't do generically since they depend on the data format -- schemas need to
    // be available if there are any non-null entries
    boolean hasKeys = false, hasValues = false;
    for (AvroProduceRecord rec : request.getRecords()) {
      hasKeys = hasKeys || !rec.getJsonKey().isNull();
      hasValues = hasValues || !rec.getJsonValue().isNull();
    }
    if (hasKeys && request.getKeySchema() == null && request.getKeySchemaId() == null) {
      throw Errors.keySchemaMissingException();
    }
    if (hasValues && request.getValueSchema() == null && request.getValueSchemaId() == null) {
      throw Errors.valueSchemaMissingException();
    }

    produce(asyncResponse, topic, partition, EmbeddedFormat.AVRO, request);
  }

  private <ClientK, ClientV> void consume(
      final @Suspended AsyncResponse asyncResponse,
      final String topicName,
      final int partitionId,
      final long offset,
      final long count,
      final EmbeddedFormat embeddedFormat) {

    ctx.getSimpleConsumerManager().consume(
        topicName, partitionId, offset, count, embeddedFormat,
        new ConsumerManager.ReadCallback<ClientK, ClientV>() {
          @Override
          public void onCompletion(List<? extends AbstractConsumerRecord<ClientK, ClientV>> records,
                                   Exception e) {
            if (e != null) {
              asyncResponse.resume(e);
            } else {
              asyncResponse.resume(records);
            }
          }
        });
  }

  protected <K, V, R extends ProduceRecord<K, V>> void produce(
      final AsyncResponse asyncResponse,
      final String topic,
      final int partition,
      final EmbeddedFormat format,
      final PartitionProduceRequest<R> request) {
    // If the topic already exists, we can proactively check for the partition
    if (topicExists(topic)) {
      if (!ctx.getMetadataObserver().partitionExists(topic, partition)) {
        throw Errors.partitionNotFoundException();
      }
    }

    ctx.getProducerPool().produce(
        topic, partition, format,
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

  private boolean topicExists(final String topic) {
    return ctx.getMetadataObserver().topicExists(topic);
  }

  private void checkTopicExists(final String topic) {
    if (!topicExists(topic)) {
      throw Errors.topicNotFoundException();
    }
  }
}
