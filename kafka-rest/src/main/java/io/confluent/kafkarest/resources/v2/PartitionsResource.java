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
import io.confluent.kafkarest.entities.Partition;
import io.confluent.kafkarest.entities.ProduceRecord;
import io.confluent.kafkarest.entities.ProduceRequest;
import io.confluent.kafkarest.entities.v2.BinaryPartitionProduceRequest;
import io.confluent.kafkarest.entities.v2.GetPartitionResponse;
import io.confluent.kafkarest.entities.v2.JsonPartitionProduceRequest;
import io.confluent.kafkarest.entities.v2.PartitionOffset;
import io.confluent.kafkarest.entities.v2.ProduceResponse;
import io.confluent.kafkarest.entities.v2.SchemaPartitionProduceRequest;
import io.confluent.kafkarest.entities.v2.TopicPartitionOffsetResponse;
import io.confluent.rest.annotations.PerformanceMetric;
import java.util.List;
import java.util.Vector;
import java.util.stream.Collectors;
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

@Path("/topics/{topic}/partitions")
@Produces({Versions.KAFKA_V2_JSON_BINARY_WEIGHTED_LOW,
           Versions.KAFKA_V2_JSON_AVRO_WEIGHTED_LOW,
           Versions.KAFKA_V2_JSON_JSON_SCHEMA_WEIGHTED_LOW,
           Versions.KAFKA_V2_JSON_PROTOBUF_WEIGHTED_LOW,
           Versions.KAFKA_V2_JSON_WEIGHTED})
@Consumes({Versions.KAFKA_V2_JSON})
public final class PartitionsResource {
  private static final Logger log = LoggerFactory.getLogger(PartitionsResource.class);

  private final KafkaRestContext ctx;

  public PartitionsResource(KafkaRestContext ctx) {
    this.ctx = ctx;
  }

  @GET
  @PerformanceMetric("partitions.list+v2")
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
  @PerformanceMetric("partition.get+v2")
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


  @POST
  @Path("/{partition}")
  @PerformanceMetric("partition.produce-binary+v2")
  @Consumes({Versions.KAFKA_V2_JSON_BINARY})
  public void produceBinary(
      final @Suspended AsyncResponse asyncResponse,
      final @PathParam("topic") String topic,
      final @PathParam("partition") int partition,
      @Valid @NotNull BinaryPartitionProduceRequest request
  )  throws Exception {
    produce(
        asyncResponse,
        topic,
        partition,
        EmbeddedFormat.BINARY,
        request.toProduceRequest());
  }

  @POST
  @Path("/{partition}")
  @PerformanceMetric("partition.produce-json+v2")
  @Consumes({Versions.KAFKA_V2_JSON_JSON})
  public void produceJson(
      final @Suspended AsyncResponse asyncResponse,
      final @PathParam("topic") String topic,
      final @PathParam("partition") int partition,
      @Valid @NotNull JsonPartitionProduceRequest request
  )  throws Exception {
    produce(
        asyncResponse,
        topic,
        partition,
        EmbeddedFormat.JSON,
        request.toProduceRequest());
  }

  @POST
  @Path("/{partition}")
  @PerformanceMetric("partition.produce-avro+v2")
  @Consumes({Versions.KAFKA_V2_JSON_AVRO})
  public void produceAvro(
      final @Suspended AsyncResponse asyncResponse,
      final @PathParam("topic") String topic,
      final @PathParam("partition") int partition,
      @Valid @NotNull SchemaPartitionProduceRequest request
  )  throws Exception {
    produceSchema(
        asyncResponse,
        topic,
        partition,
        request.toProduceRequest(),
        EmbeddedFormat.AVRO);
  }

  @POST
  @Path("/{partition}")
  @PerformanceMetric("partition.produce-jsonschema+v2")
  @Consumes({Versions.KAFKA_V2_JSON_JSON_SCHEMA})
  public void produceJsonSchema(
      final @Suspended AsyncResponse asyncResponse,
      final @PathParam("topic") String topic,
      final @PathParam("partition") int partition,
      @Valid @NotNull SchemaPartitionProduceRequest request
  )  throws Exception {
    // Validations we can't do generically since they depend on the data format -- schemas need to
    // be available if there are any non-null entries
    produceSchema(
        asyncResponse,
        topic,
        partition,
        request.toProduceRequest(),
        EmbeddedFormat.JSONSCHEMA);
  }

  @POST
  @Path("/{partition}")
  @PerformanceMetric("partition.produce-protobuf+v2")
  @Consumes({Versions.KAFKA_V2_JSON_PROTOBUF})
  public void produceProtobuf(
      final @Suspended AsyncResponse asyncResponse,
      final @PathParam("topic") String topic,
      final @PathParam("partition") int partition,
      @Valid @NotNull SchemaPartitionProduceRequest request
  )  throws Exception {
    // Validations we can't do generically since they depend on the data format -- schemas need to
    // be available if there are any non-null entries
    produceSchema(
        asyncResponse,
        topic,
        partition,
        request.toProduceRequest(),
        EmbeddedFormat.PROTOBUF);
  }

  protected <K, V> void produce(
      final AsyncResponse asyncResponse,
      final String topic,
      final int partition,
      final EmbeddedFormat format,
      final ProduceRequest<K, V> request
  ) throws Exception {
    // If the topic already exists, we can proactively check for the partition
    if (topicExists(topic)) {
      if (!ctx.getAdminClientWrapper().partitionExists(topic, partition)) {
        throw Errors.partitionNotFoundException();
      }
    }

    log.trace(
        "Executing topic produce request id={} topic={} partition={} format={} request={}",
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
            List<PartitionOffset> offsets = new Vector<PartitionOffset>();
            for (RecordMetadataOrException result : results) {
              if (result.getException() != null) {
                int errorCode =
                    Utils.errorCodeFromProducerException(result.getException());
                String errorMessage = result.getException().getMessage();
                offsets.add(new PartitionOffset(null, null, errorCode, errorMessage));
              } else {
                offsets.add(new PartitionOffset(
                    result.getRecordMetadata().partition(),
                    result.getRecordMetadata().offset(),
                    null,
                    null
                ));
              }
            }
            ProduceResponse response = new ProduceResponse(offsets, keySchemaId, valueSchemaId);
            log.trace(
                "Completed topic produce request id={} response={}",
                asyncResponse, response
            );
            Response.Status requestStatus = response.getRequestStatus();
            asyncResponse.resume(Response.status(requestStatus).entity(response).build());
          }
        }
    );
  }

  private void produceSchema(
      AsyncResponse asyncResponse,
      String topic,
      int partition,
      ProduceRequest<JsonNode, JsonNode> request,
      EmbeddedFormat avro
  ) throws Exception {
    checkKeySchema(request);
    checkValueSchema(request);
    produce(asyncResponse, topic, partition, avro, request);
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

  /**
   * Returns a summary with beginning and end offsets for the given {@code topic} and {@code
   * partition}.
   *
   * @throws io.confluent.rest.exceptions.RestNotFoundException if either {@code topic} or {@code
   *                                                            partition} don't exist.
   */
  @GET
  @Path("/{partition}/offsets")
  public TopicPartitionOffsetResponse getOffsets(
      @PathParam("topic") String topic,
      @PathParam("partition") int partition
  ) throws Exception {
    checkTopicExists(topic);
    checkPartitionExists(topic, partition);

    return new TopicPartitionOffsetResponse(
        getBeginningOffset(topic, partition), getEndOffset(topic, partition));
  }

  /**
   * Returns the earliest offset in the {@code topic} {@code partition}.
   */
  private long getBeginningOffset(String topic, int partition) {
    return ctx.getKafkaConsumerManager().getBeginningOffset(topic, partition);
  }

  /**
   * Returns the latest offset in the {@code topic} {@code partition}.
   */
  private long getEndOffset(String topic, int partition) {
    return ctx.getKafkaConsumerManager().getEndOffset(topic, partition);
  }

  private void checkTopicExists(String topic) throws Exception {
    if (!topicExists(topic)) {
      throw Errors.topicNotFoundException();
    }
  }

  private boolean topicExists(String topic) throws Exception {
    return ctx.getAdminClientWrapper().topicExists(topic);
  }

  private void checkPartitionExists(String topic, int partition) throws Exception {
    if (!ctx.getAdminClientWrapper().partitionExists(topic, partition)) {
      throw Errors.partitionNotFoundException();
    }
  }
}
