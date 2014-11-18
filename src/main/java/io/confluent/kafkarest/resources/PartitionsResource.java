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
import io.confluent.kafkarest.entities.Partition;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.util.List;

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
}
