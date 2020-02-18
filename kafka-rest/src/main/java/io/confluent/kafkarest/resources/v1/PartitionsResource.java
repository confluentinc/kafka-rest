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

package io.confluent.kafkarest.resources.v1;

import static java.util.Collections.emptyList;

import io.confluent.kafkarest.ConsumerReadCallback;
import io.confluent.kafkarest.Errors;
import io.confluent.kafkarest.KafkaRestContext;
import io.confluent.kafkarest.ProducerPool;
import io.confluent.kafkarest.RecordMetadataOrException;
import io.confluent.kafkarest.Utils;
import io.confluent.kafkarest.Versions;
import io.confluent.kafkarest.entities.ConsumerRecord;
import io.confluent.kafkarest.entities.EmbeddedFormat;
import io.confluent.kafkarest.entities.Partition;
import io.confluent.kafkarest.entities.ProduceRequest;
import io.confluent.kafkarest.entities.v1.AvroConsumerRecord;
import io.confluent.kafkarest.entities.v1.AvroPartitionProduceRequest;
import io.confluent.kafkarest.entities.v1.AvroPartitionProduceRequest.AvroPartitionProduceRecord;
import io.confluent.kafkarest.entities.v1.BinaryConsumerRecord;
import io.confluent.kafkarest.entities.v1.BinaryPartitionProduceRequest;
import io.confluent.kafkarest.entities.v1.GetPartitionResponse;
import io.confluent.kafkarest.entities.v1.JsonConsumerRecord;
import io.confluent.kafkarest.entities.v1.JsonPartitionProduceRequest;
import io.confluent.kafkarest.entities.v1.PartitionOffset;
import io.confluent.kafkarest.entities.v1.ProduceResponse;
import io.confluent.rest.annotations.PerformanceMetric;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.Vector;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/topics/{topic}/partitions")
@Produces({Versions.KAFKA_V1_JSON_BINARY_WEIGHTED_LOW, Versions.KAFKA_V1_JSON_AVRO_WEIGHTED_LOW,
           Versions.KAFKA_V1_JSON_WEIGHTED, Versions.KAFKA_DEFAULT_JSON_WEIGHTED,
           Versions.JSON_WEIGHTED})
@Consumes({Versions.KAFKA_V1_JSON, Versions.KAFKA_DEFAULT_JSON, Versions.JSON,
           Versions.GENERIC_REQUEST})
public class PartitionsResource {

  private static final Logger log = LoggerFactory.getLogger(PartitionsResource.class);

  private final KafkaRestContext ctx;

  public PartitionsResource(KafkaRestContext ctx) {
    this.ctx = ctx;
  }

  @GET
  @PerformanceMetric("partitions.list")
  public List<GetPartitionResponse> list(final @PathParam("topic") String topic) throws Exception {
    checkTopicExists(topic);
    return ctx.getAdminClientWrapper()
        .getTopicPartitions(topic)
        .stream()
        .map(GetPartitionResponse::fromPartition)
        .collect(Collectors.toList());
  }

  @GET
  @Path("/{partition}")
  @PerformanceMetric("partition.get")
  public GetPartitionResponse getPartition(
      final @PathParam("topic") String topic,
      @PathParam("partition") int partition
  ) throws Exception {
    checkTopicExists(topic);
    Partition part = ctx.getAdminClientWrapper().getTopicPartition(topic, partition);
    if (part == null) {
      throw Errors.partitionNotFoundException();
    }
    return GetPartitionResponse.fromPartition(part);
  }

  @GET
  @Path("/{partition}/messages")
  @PerformanceMetric("partition.consume-binary")
  @Produces({Versions.KAFKA_V1_JSON_BINARY_WEIGHTED,
             Versions.KAFKA_V1_JSON_WEIGHTED,
             Versions.KAFKA_DEFAULT_JSON_WEIGHTED,
             Versions.JSON_WEIGHTED})
  public void consumeBinary(
      @Suspended AsyncResponse asyncResponse,
      @PathParam("topic") String topicName,
      @PathParam("partition") int partitionId,
      @QueryParam("offset") @Nullable Long offset,
      @QueryParam("timestamp") @Nullable Instant timestamp,
      @QueryParam("count") @DefaultValue("1") long count
  ) {
    if ((offset != null) == (timestamp != null)) {
      throw new BadRequestException(
          "Either `offset` or `timestamp` query parameters must be set.");
    }

    if (offset != null) {
      consume(
          asyncResponse,
          topicName,
          partitionId,
          offset,
          count,
          EmbeddedFormat.BINARY,
          BinaryConsumerRecord::fromConsumerRecord);
    } else {
      consume(
          asyncResponse,
          topicName,
          partitionId,
          timestamp,
          count,
          EmbeddedFormat.BINARY,
          BinaryConsumerRecord::fromConsumerRecord);
    }
  }

  @GET
  @Path("/{partition}/messages")
  @PerformanceMetric("partition.consume-avro")
  @Produces({Versions.KAFKA_V1_JSON_AVRO_WEIGHTED_LOW})
  public void consumeAvro(
      @Suspended AsyncResponse asyncResponse,
      @PathParam("topic") String topicName,
      @PathParam("partition") int partitionId,
      @QueryParam("offset") @Nullable Long offset,
      @QueryParam("timestamp") @Nullable Instant timestamp,
      @QueryParam("count") @DefaultValue("1") long count
  ) {
    if ((offset != null) == (timestamp != null)) {
      throw new BadRequestException(
          "Either `offset` or `timestamp` query parameters must be set.");
    }

    if (offset != null) {
      consume(
          asyncResponse,
          topicName,
          partitionId,
          offset,
          count,
          EmbeddedFormat.AVRO,
          AvroConsumerRecord::fromConsumerRecord);
    } else {
      consume(
          asyncResponse,
          topicName,
          partitionId,
          timestamp,
          count,
          EmbeddedFormat.AVRO,
          AvroConsumerRecord::fromConsumerRecord);
    }
  }

  @GET
  @Path("/{partition}/messages")
  @PerformanceMetric("partition.consume-json")
  @Produces({Versions.KAFKA_V1_JSON_JSON_WEIGHTED_LOW})
  public void consumeJson(
      @Suspended AsyncResponse asyncResponse,
      @PathParam("topic") String topicName,
      @PathParam("partition") int partitionId,
      @QueryParam("offset") @Nullable Long offset,
      @QueryParam("timestamp") @Nullable Instant timestamp,
      @QueryParam("count") @DefaultValue("1") long count
  ) {
    if ((offset != null) == (timestamp != null)) {
      throw new BadRequestException(
          "Either `offset` or `timestamp` query parameters must be set.");
    }

    if (offset != null) {
      consume(
          asyncResponse,
          topicName,
          partitionId,
          offset,
          count,
          EmbeddedFormat.JSON,
          JsonConsumerRecord::fromConsumerRecord);
    } else {
      consume(
          asyncResponse,
          topicName,
          partitionId,
          timestamp,
          count,
          EmbeddedFormat.JSON,
          JsonConsumerRecord::fromConsumerRecord);
    }
  }

  @POST
  @Path("/{partition}")
  @PerformanceMetric("partition.produce-binary")
  @Consumes({Versions.KAFKA_V1_JSON_BINARY, Versions.KAFKA_V1_JSON,
             Versions.KAFKA_DEFAULT_JSON, Versions.JSON, Versions.GENERIC_REQUEST})
  public void produceBinary(
      final @Suspended AsyncResponse asyncResponse,
      final @PathParam("topic") String topic,
      final @PathParam("partition") int partition,
      @Valid @NotNull BinaryPartitionProduceRequest request
  ) {
    produce(
        asyncResponse,
        topic,
        partition,
        EmbeddedFormat.BINARY,
        request.toProduceRequest());
  }

  @POST
  @Path("/{partition}")
  @PerformanceMetric("partition.produce-json")
  @Consumes({Versions.KAFKA_V1_JSON_JSON})
  public void produceJson(
      final @Suspended AsyncResponse asyncResponse,
      final @PathParam("topic") String topic,
      final @PathParam("partition") int partition,
      @Valid @NotNull JsonPartitionProduceRequest request
  ) {
    produce(
        asyncResponse,
        topic,
        partition,
        EmbeddedFormat.JSON,
        request.toProduceRequest());
  }

  @POST
  @Path("/{partition}")
  @PerformanceMetric("partition.produce-avro")
  @Consumes({Versions.KAFKA_V1_JSON_AVRO})
  public void produceAvro(
      final @Suspended AsyncResponse asyncResponse,
      final @PathParam("topic") String topic,
      final @PathParam("partition") int partition,
      @Valid @NotNull AvroPartitionProduceRequest request
  ) {
    // Validations we can't do generically since they depend on the data format -- schemas need to
    // be available if there are any non-null entries
    boolean hasKeys = false;
    boolean hasValues = false;
    for (AvroPartitionProduceRecord rec : request.getRecords()) {
      hasKeys = hasKeys || !rec.getKey().isNull();
      hasValues = hasValues || !rec.getValue().isNull();
    }
    if (hasKeys && request.getKeySchema() == null && request.getKeySchemaId() == null) {
      throw Errors.keySchemaMissingException();
    }
    if (hasValues && request.getValueSchema() == null && request.getValueSchemaId() == null) {
      throw Errors.valueSchemaMissingException();
    }

    produce(
        asyncResponse,
        topic,
        partition,
        EmbeddedFormat.AVRO,
        request.toProduceRequest());
  }

  private <K, V> void consume(
      @Suspended AsyncResponse asyncResponse,
      String topicName,
      int partitionId,
      Instant timestamp,
      long count,
      EmbeddedFormat embeddedFormat,
      Function<ConsumerRecord<K, V>, ?> toJsonWrapper
  ) {
    Optional<Long> offset =
        ctx.getKafkaConsumerManager().getOffsetForTime(topicName, partitionId, timestamp);

    if (offset.isPresent()) {
      consume(
          asyncResponse,
          topicName,
          partitionId,
          offset.get(),
          count,
          embeddedFormat,
          toJsonWrapper);
    } else {
      // No messages at or after timestamp. Return empty.
      asyncResponse.resume(emptyList());
    }
  }

  private <K, V> void consume(
      final @Suspended AsyncResponse asyncResponse,
      final String topicName,
      final int partitionId,
      final long offset,
      final long count,
      final EmbeddedFormat embeddedFormat,
      Function<ConsumerRecord<K, V>, ?> toJsonWrapper
  ) {

    log.trace("Executing simple consume id={} topic={} partition={} offset={} count={}",
        asyncResponse, topicName, partitionId, offset, count
    );

    ctx.getSimpleConsumerManager().consume(
        topicName, partitionId, offset, count, embeddedFormat,
        new ConsumerReadCallback<K, V>() {
          @Override
          public void onCompletion(
              List<ConsumerRecord<K, V>> records,
              Exception e
          ) {
            log.trace(
                "Completed simple consume id={} records={} exception={}",
                asyncResponse,
                records,
                e
            );
            if (e != null) {
              asyncResponse.resume(e);
            } else {
              asyncResponse.resume(
                  records.stream().map(toJsonWrapper).collect(Collectors.toList()));
            }
          }
        }
    );
  }

  protected <K, V> void produce(
      final AsyncResponse asyncResponse,
      final String topic,
      final int partition,
      final EmbeddedFormat format,
      final ProduceRequest<K, V> request
  ) {

    log.trace("Executing topic produce request id={} topic={} partition={} format={} request={}",
        asyncResponse, topic, partition, format, request
    );

    ctx.getProducerPool().produce(
        topic, partition, format,
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
            Response.Status requestStatus = response.getRequestStatus();
            asyncResponse.resume(Response.status(requestStatus).entity(response).build());
          }
        }
    );
  }

  private boolean topicExists(final String topic) throws Exception {
    return ctx.getAdminClientWrapper().topicExists(topic);
  }

  private void checkTopicExists(final String topic) throws Exception {
    if (!topicExists(topic)) {
      throw Errors.topicNotFoundException();
    }
  }
}
