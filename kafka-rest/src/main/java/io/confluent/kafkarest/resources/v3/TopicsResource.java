package io.confluent.kafkarest.resources.v3;

import io.confluent.kafkarest.Versions;
import io.confluent.kafkarest.controllers.TopicManager;
import io.confluent.kafkarest.entities.Topic;
import io.confluent.kafkarest.entities.TopicsMap;
import io.confluent.kafkarest.entities.TopicsMapData;
import io.confluent.kafkarest.entities.v3.Relationship;
import io.confluent.kafkarest.entities.v3.ResourceLink;
import io.confluent.kafkarest.entities.v3.TopicData;
import io.confluent.kafkarest.response.UrlFactory;

import javax.annotation.Generated;
import javax.inject.Inject;
import javax.management.relation.Relation;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;

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
        topicManager.listTopics(clusterId);
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

    private TopicsMapData toTopicsMapData(TopicsMap topicsMap) {
        Relationship topicsMapRelationship =
                new Relationship(urlFactory.create("v3", "topics", topicsMap.getTopicsMap().toString(), "topicsMap"));
        return new TopicsMapData(
                new ResourceLink((urlFactory.create("v3", "topicsMap"))),
                topicsMap,
                topicsMapRelationship);
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
