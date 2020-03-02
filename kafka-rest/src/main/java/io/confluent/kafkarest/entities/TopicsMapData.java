package io.confluent.kafkarest.entities;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.confluent.kafkarest.entities.v3.ClusterData;
import io.confluent.kafkarest.entities.v3.Relationship;
import io.confluent.kafkarest.entities.v3.ResourceLink;
import io.confluent.kafkarest.entities.v3.TopicData;
import org.w3c.dom.Attr;

import java.util.Objects;
import java.util.StringJoiner;
import java.util.jar.Attributes;

public class TopicsMapData {
    private final ResourceLink links;

    private final Attributes attributes;

    private final Relationships relationships;

    public TopicsMapData(
            ResourceLink links,
            TopicsMap map,
            Relationship topicsMap
    ) {
        this.links = links;
        // check what attributes should be for listTopics
        this.attributes = new Attributes(map);
        this.relationships = new Relationships(topicsMap);
    }

    @JsonProperty("topicsMap")
    public TopicsMap topic() {
        return attributes.getTopicsMap();
    }

    @JsonProperty("links")
    public ResourceLink getLinks() {
        return links;
    }

    @JsonProperty("attributes")
    public Attributes getAttributes() {
        return attributes;
    }

    @JsonProperty("relationships")
    public Relationships getRelationships() {
        return relationships;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        TopicsMapData that = (TopicsMapData) o;
        return Objects.equals(links, that.links)
                && Objects.equals(attributes, that.attributes)
                && Objects.equals(relationships, that.relationships);
    }

    @Override
    public int hashCode() {
        return Objects.hash(links, attributes, relationships);
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", ClusterData.class.getSimpleName() + "[", "]")
                .add("links=" + links)
                .add("attributes=" + attributes)
                .add("relationships=" + relationships)
                .toString();
    }

    public static final class Attributes {
        private final TopicsMap topicsMap;

        public Attributes(TopicsMap topicsMap) {
            this.topicsMap = topicsMap;
        }

        @JsonProperty("topicsMap")
        public TopicsMap getTopicsMap() {
            return topicsMap;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            TopicsMapData.Attributes that = (TopicsMapData.Attributes) o;
            return Objects.equals(topicsMap, that.topicsMap);
        }

        @Override
        public int hashCode() {
            return Objects.hash(topicsMap);
        }

        @Override
        public String toString() {
            return new StringJoiner(", ", TopicsMapData.Attributes.class.getSimpleName() + "[", "]")
                    .add("topicsMap='" + topicsMap + "'")
                    .toString();
        }
    }

    public static final class Relationships {
        private final Relationship topicsMap;

        public Relationships (Relationship topicsMap) {
            this.topicsMap = topicsMap;
        }

        @JsonProperty("topicsMap")
        public Relationship getTopicsMap() {
            return topicsMap;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Relationships that = (Relationships) o;
            return Objects.equals(topicsMap, that.topicsMap);
        }

        @Override
        public int hashCode() {
            return Objects.hash(topicsMap);
        }

        @Override
        public String toString() {
            return new StringJoiner(", ", Relationships.class.getSimpleName() + "[", "]")
                    .add("TopicsMapData="+ topicsMap)
                    .toString();
        }
    }
}
