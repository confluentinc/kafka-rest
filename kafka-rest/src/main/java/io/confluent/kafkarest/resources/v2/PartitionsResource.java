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

import io.confluent.kafkarest.Errors;
import io.confluent.kafkarest.KafkaRestContext;
import io.confluent.kafkarest.ProducerPool;
import io.confluent.kafkarest.RecordMetadataOrException;
import io.confluent.kafkarest.Utils;
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
import io.confluent.kafkarest.entities.TopicPartitionOffsetResponse;
import io.confluent.rest.annotations.PerformanceMetric;
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

@Path("/topics/{topic}/partitions")
@Produces({Versions.KAFKA_V2_JSON_BINARY_WEIGHTED_LOW, Versions.KAFKA_V2_JSON_AVRO_WEIGHTED_LOW,
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
  public List<Partition> list(final @PathParam("topic") String topic) throws Exception {
    checkTopicExists(topic);
    return ctx.getAdminClientWrapper().getTopicPartitions(topic);
  }

  @GET
  @Path("/{partition}")
  @PerformanceMetric("partition.get+v2")
  public Partition getPartition(
      final @PathParam("topic") String topic,
      @PathParam("partition") int partition
  )  throws Exception {
    checkTopicExists(topic);
    Partition part = ctx.getAdminClientWrapper().getTopicPartition(topic, partition);
    if (part == null) {
      throw Errors.partitionNotFoundException();
    }
    return part;
  }


  @POST
  @Path("/{partition}")
  @PerformanceMetric("partition.produce-binary+v2")
  @Consumes({Versions.KAFKA_V2_JSON_BINARY})
  public void produceBinary(
      final @Suspended AsyncResponse asyncResponse,
      final @PathParam("topic") String topic,
      final @PathParam("partition") int partition,
      @Valid @NotNull PartitionProduceRequest<BinaryProduceRecord> request
  )  throws Exception {
    produce(asyncResponse, topic, partition, EmbeddedFormat.BINARY, request);
  }

  @POST
  @Path("/{partition}")
  @PerformanceMetric("partition.produce-json+v2")
  @Consumes({Versions.KAFKA_V2_JSON_JSON})
  public void produceJson(
      final @Suspended AsyncResponse asyncResponse,
      final @PathParam("topic") String topic,
      final @PathParam("partition") int partition,
      @Valid @NotNull PartitionProduceRequest<JsonProduceRecord> request
  )  throws Exception {
    produce(asyncResponse, topic, partition, EmbeddedFormat.JSON, request);
  }

  @POST
  @Path("/{partition}")
  @PerformanceMetric("partition.produce-avro+v2")
  @Consumes({Versions.KAFKA_V2_JSON_AVRO})
  public void produceAvro(
      final @Suspended AsyncResponse asyncResponse,
      final @PathParam("topic") String topic,
      final @PathParam("partition") int partition,
      @Valid @NotNull PartitionProduceRequest<AvroProduceRecord> request
  )  throws Exception {
    // Validations we can't do generically since they depend on the data format -- schemas need to
    // be available if there are any non-null entries
    boolean hasKeys = false;
    boolean hasValues = false;
    for (AvroProduceRecord rec : request.getRecords()) {
      hasKeys = hasKeys || rec.hasKey();
      hasValues = hasValues || rec.hasValue();
    }
    if (hasKeys && request.getKeySchema() == null && request.getKeySchemaId() == null) {
      throw Errors.keySchemaMissingException();
    }
    if (hasValues && request.getValueSchema() == null && request.getValueSchemaId() == null) {
      throw Errors.valueSchemaMissingException();
    }

    produce(asyncResponse, topic, partition, EmbeddedFormat.AVRO, request);
  }

  protected <K, V, R extends ProduceRecord<K, V>> void produce(
      final AsyncResponse asyncResponse,
      final String topic,
      final int partition,
      final EmbeddedFormat format,
      final PartitionProduceRequest<R> request
  )  throws Exception {
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
            response.setOffsets(offsets);
            response.setKeySchemaId(keySchemaId);
            response.setValueSchemaId(valueSchemaId);
            log.trace(
                "Completed topic produce request id={} response={}",
                asyncResponse, response
            );
            Response.Status requestStatus = Utils.produceRequestStatus(response);
            asyncResponse.resume(Response.status(requestStatus).entity(response).build());
          }
        }
    );
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
