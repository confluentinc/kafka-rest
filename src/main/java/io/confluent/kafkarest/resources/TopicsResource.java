package io.confluent.kafkarest.resources;

import io.confluent.kafkarest.Context;
import io.confluent.kafkarest.entities.Topic;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.util.List;

@Path("/topics")
@Produces(MediaType.APPLICATION_JSON)
public class TopicsResource {
    private final Context ctx;

    public TopicsResource(Context ctx) {
        this.ctx = ctx;
    }

    @GET
    public List<Topic> list() {
        return ctx.getMetadataObserver().getTopics();
    }

    @GET
    @Path("/{topic}")
    public Topic getTopic(@PathParam("topic") String topicName) {
        Topic topic = ctx.getMetadataObserver().getTopic(topicName);
        if (topic == null)
            throw new NotFoundException();
        return topic;
    }
}
