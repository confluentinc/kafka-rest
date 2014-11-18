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
import io.confluent.kafkarest.ProducerPool;
import io.confluent.kafkarest.ProducerRecordProxyCollection;
import io.confluent.kafkarest.entities.Partition;
import io.confluent.kafkarest.entities.PartitionProduceRequest;
import io.confluent.kafkarest.entities.ProduceResponse;

import javax.validation.Valid;
import javax.ws.rs.*;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.MediaType;
import java.util.List;
import java.util.Map;
import java.util.Vector;

@Produces(MediaType.APPLICATION_JSON)
public class PartitionsResource {
    private final Context ctx;
    private final String topic;

    public PartitionsResource(Context ctx, String topic) {
        this.ctx = ctx;
        this.topic = topic;
    }

    @GET
    public List<Partition> list() {
        return ctx.getMetadataObserver().getTopicPartitions(topic);
    }

    @GET
    @Path("/{partition}")
    public Partition getPartition(@PathParam("partition") int partition) {
        Partition part = ctx.getMetadataObserver().getTopicPartition(topic, partition);
        if (part == null)
            throw new NotFoundException();
        return part;
    }

    @POST
    @Path("/{partition}")
    public void produce(final @Suspended AsyncResponse asyncResponse, @PathParam("partition") int partition, @Valid PartitionProduceRequest request) {
        if (!ctx.getMetadataObserver().partitionExists(topic, partition))
            throw new NotFoundException();

        ctx.getProducerPool().produce(
                new ProducerRecordProxyCollection(topic, partition, request.getRecords()),
                new ProducerPool.ProduceRequestCallback() {
                    public void onCompletion(Map<Integer, Long> partitionOffsets) {
                        ProduceResponse response = new ProduceResponse();
                        List<ProduceResponse.PartitionOffset> offsets = new Vector<ProduceResponse.PartitionOffset>();
                        for (Map.Entry<Integer, Long> partOff : partitionOffsets.entrySet()) {
                            offsets.add(new ProduceResponse.PartitionOffset(partOff.getKey(), partOff.getValue()));
                        }
                        response.setOffsets(offsets);
                        asyncResponse.resume(response);
                    }

                    public void onException(Exception e) {
                        asyncResponse.resume(e);
                    }
                }
        );
    }
}
