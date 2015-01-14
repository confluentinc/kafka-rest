/**
 * Copyright 2014 Confluent Inc.
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
 */
package io.confluent.kafkarest.resources;

import io.confluent.kafkarest.Context;
import io.confluent.kafkarest.Errors;
import io.confluent.kafkarest.ProducerPool;
import io.confluent.kafkarest.ProducerRecordProxyCollection;
import io.confluent.kafkarest.Versions;
import io.confluent.kafkarest.entities.Partition;
import io.confluent.kafkarest.entities.PartitionOffset;
import io.confluent.kafkarest.entities.PartitionProduceRequest;

import javax.validation.Valid;
import javax.ws.rs.*;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import java.util.List;
import java.util.Map;

@Produces({Versions.KAFKA_V1_JSON_WEIGHTED, Versions.KAFKA_DEFAULT_JSON_WEIGHTED, Versions.JSON_WEIGHTED})
@Consumes({Versions.KAFKA_V1_JSON, Versions.KAFKA_DEFAULT_JSON, Versions.JSON, Versions.GENERIC_REQUEST})
public class PartitionsResource {
    private final Context ctx;
    private final String topic;

    public PartitionsResource(Context ctx, String topic) {
        this.ctx = ctx;
        this.topic = topic;
    }

    @GET
    public List<Partition> list() {
        checkTopicExists();
        return ctx.getMetadataObserver().getTopicPartitions(topic);
    }

    @GET
    @Path("/{partition}")
    public Partition getPartition(@PathParam("partition") int partition) {
        checkTopicExists();
        Partition part = ctx.getMetadataObserver().getTopicPartition(topic, partition);
        if (part == null)
            throw Errors.partitionNotFoundException();
        return part;
    }

    @POST
    @Path("/{partition}")
    public void produce(final @Suspended AsyncResponse asyncResponse, final @PathParam("partition") int partition, @Valid PartitionProduceRequest request) {
        checkTopicExists();
        if (!ctx.getMetadataObserver().partitionExists(topic, partition))
            throw Errors.partitionNotFoundException();

        ctx.getProducerPool().produce(
                new ProducerRecordProxyCollection(topic, partition, request.getRecords()),
                new ProducerPool.ProduceRequestCallback() {
                    public void onCompletion(Map<Integer, Long> partitionOffsets) {
                        PartitionOffset response = new PartitionOffset(partition, partitionOffsets.get(partition));
                        asyncResponse.resume(response);
                    }

                    public void onException(Exception e) {
                        asyncResponse.resume(e);
                    }
                }
        );
    }

    private void checkTopicExists() {
        if (!ctx.getMetadataObserver().topicExists(topic))
            throw Errors.topicNotFoundException();
    }
}
