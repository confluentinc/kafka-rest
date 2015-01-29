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
import java.util.Map;

import javax.validation.Valid;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;

import io.confluent.kafkarest.Context;
import io.confluent.kafkarest.Errors;
import io.confluent.kafkarest.ProducerPool;
import io.confluent.kafkarest.ProducerRecordProxyCollection;
import io.confluent.kafkarest.Versions;
import io.confluent.kafkarest.entities.Partition;
import io.confluent.kafkarest.entities.PartitionOffset;
import io.confluent.kafkarest.entities.PartitionProduceRequest;
import io.confluent.rest.annotations.PerformanceMetric;

@Path("/topics/{topic}/partitions")
@Produces({Versions.KAFKA_V1_JSON_WEIGHTED, Versions.KAFKA_DEFAULT_JSON_WEIGHTED,
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

  @POST
  @Path("/{partition}")
  @PerformanceMetric("partition.produce")
  public void produce(final @Suspended AsyncResponse asyncResponse,
                      final @PathParam("topic") String topic,
                      final @PathParam("partition") int partition,
                      @Valid PartitionProduceRequest request) {
    checkTopicExists(topic);
    if (!ctx.getMetadataObserver().partitionExists(topic, partition)) {
      throw Errors.partitionNotFoundException();
    }

    ctx.getProducerPool().produce(
        new ProducerRecordProxyCollection(topic, partition, request.getRecords()),
        new ProducerPool.ProduceRequestCallback() {
          public void onCompletion(Map<Integer, Long> partitionOffsets) {
            PartitionOffset
                response =
                new PartitionOffset(partition, partitionOffsets.get(partition));
            asyncResponse.resume(response);
          }

          public void onException(Exception e) {
            asyncResponse.resume(e);
          }
        }
    );
  }

  private void checkTopicExists(final String topic) {
    if (!ctx.getMetadataObserver().topicExists(topic)) {
      throw Errors.topicNotFoundException();
    }
  }
}
