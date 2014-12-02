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

import io.confluent.kafkarest.ConsumerManager;
import io.confluent.kafkarest.Context;
import io.confluent.kafkarest.Versions;
import io.confluent.kafkarest.entities.ConsumerRecord;
import io.confluent.kafkarest.entities.CreateConsumerInstanceResponse;

import javax.validation.Valid;
import javax.ws.rs.*;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.UriInfo;
import java.util.List;

@Path("/consumers")
@Produces({Versions.KAFKA_V1_JSON_WEIGHTED, Versions.KAFKA_DEFAULT_JSON_WEIGHTED, Versions.JSON_WEIGHTED})
public class ConsumersResource {
    private final Context ctx;

    public ConsumersResource(Context ctx) {
        this.ctx = ctx;
    }

    @POST
    @Valid
    @Path("/{group}")
    public CreateConsumerInstanceResponse createGroup(@javax.ws.rs.core.Context UriInfo uriInfo, final @PathParam("group") String group) {
        String instanceId = ctx.getConsumerManager().createConsumer(group);
        String instanceBaseUri = uriInfo.getAbsolutePathBuilder().path("instances").path(instanceId).build().toString();
        return new CreateConsumerInstanceResponse(instanceId, instanceBaseUri);
    }

    @DELETE
    @Path("/{group}/instances/{instance}")
    public void createGroup(final @PathParam("group") String group, final @PathParam("instance") String instance) {
        ctx.getConsumerManager().deleteConsumer(group, instance);
    }

    @GET
    @Path("/{group}/instances/{instance}/topics/{topic}")
    public void readTopic(final @Suspended AsyncResponse asyncResponse, final @PathParam("group") String group,
                          final @PathParam("instance") String instance, final @PathParam("topic") String topic)
    {
        ctx.getConsumerManager().readTopic(group, instance, topic, new ConsumerManager.ReadCallback() {
            @Override
            public void onCompletion(List<ConsumerRecord> records, Exception e) {
                if (e != null)
                    asyncResponse.resume(e);
                else
                    asyncResponse.resume(records);
            }
        });
    }
}
