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
import io.confluent.kafkarest.entities.BrokerList;

import javax.validation.Valid;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

/**
 * Resource representing the collection of all available brokers.
 */
@Path("/brokers")
@Produces(MediaType.APPLICATION_JSON)
public class BrokersResource {
    private final Context ctx;

    public BrokersResource(Context ctx) {
        this.ctx = ctx;
    }

    @GET
    @Valid
    public BrokerList list() {
        return new BrokerList(ctx.getMetadataObserver().getBrokerIds());
    }
}
