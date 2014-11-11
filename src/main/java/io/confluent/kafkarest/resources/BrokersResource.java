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
