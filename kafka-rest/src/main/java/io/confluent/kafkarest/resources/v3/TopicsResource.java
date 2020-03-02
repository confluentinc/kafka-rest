package io.confluent.kafkarest.resources.v3;

import io.confluent.kafkarest.Versions;
import io.confluent.kafkarest.controllers.TopicManager;
import io.confluent.kafkarest.entities.Topic;
import io.confluent.kafkarest.entities.v3.TopicInfoData;
import io.confluent.kafkarest.entities.v3.CollectionLink;
import io.confluent.kafkarest.entities.v3.ListTopicsResponse;
import io.confluent.kafkarest.entities.v3.Relationship;
import io.confluent.kafkarest.entities.v3.ResourceLink;
import io.confluent.kafkarest.entities.v3.TopicData;
import io.confluent.kafkarest.response.UrlFactory;
import org.apache.kafka.clients.admin.TopicListing;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import java.util.concurrent.CompletionException;
import java.util.stream.Collectors;

@Path("/v3/clusters/{clusterId}/topics")
public final class TopicsResource {
    private final TopicManager topicManager;
    private final UrlFactory urlFactory;

    @Inject
    public TopicsResource(TopicManager topicManager, UrlFactory urlFactory) {
        this.topicManager = topicManager;
        this.urlFactory = urlFactory;
    }

    @GET
    @Produces(Versions.JSON_API)
    public void listTopics(@Suspended AsyncResponse asyncResponse,
                           @PathParam("clusterId") String clusterId) {
        topicManager.listTopics(clusterId)
            .thenApply(
                    topics ->
                            new ListTopicsResponse(
                                    new CollectionLink(urlFactory.create("v3", "topics"), null),
                                    topics.stream().map(this::toTopicData).collect(Collectors.toList())))
            .whenComplete(
                    (response, exception) -> {
                        if (exception == null) {
                            asyncResponse.resume(response);
                        } else if (exception instanceof CompletionException) {
                            asyncResponse.resume(exception.getCause());
                        } else {
                            asyncResponse.resume(exception);
                        }
                    });
        //todo develop this logic
    }

    @GET
    @Path("/{topicName}")
    @Produces(Versions.JSON_API)
    public void getTopic(@Suspended AsyncResponse asyncResponse,
        @PathParam("clusterId") String clusterId,
        @PathParam("topicName") String topicName) {
        topicManager.getTopic(clusterId, topicName);
        //todo develop remaining logic
    }

    private TopicInfoData toTopicInfoData(TopicListing topicListing) {
        Relationship topicRelationship =
                new Relationship(urlFactory.create("v3", "topics", topicListing.name(), "topicListing"));
        return new TopicInfoData(
                new ResourceLink((urlFactory.create("v3", "topics", topicListing.name()))),
                topicListing.name(),
                topicRelationship);
    }

    private TopicData toTopicData(Topic topic) {
        Relationship configs =
                new Relationship(urlFactory.create("v3", "topic", topic.getName(), "configs"));
        Relationship replicationFactor = new Relationship(urlFactory.create("v3", "topic", topic.getName(), "replicationFactor"));

        Relationship isInternal =
                new Relationship(urlFactory.create("v3", "topic", topic.getName(), "isInternal"));
        Relationship partitions =
                new Relationship((urlFactory.create("v3", "topic", topic.getName(), "cluster")));

        return new TopicData(
                new ResourceLink(urlFactory.create("v3", "topic", topic.getName())),
                topic.getName(),
                isInternal,
                replicationFactor,
                configs,
                partitions);
    }

}
