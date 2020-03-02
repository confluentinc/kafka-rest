package io.confluent.kafkarest.entities.v3;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;
import java.util.StringJoiner;

public class TopicInfoData {
    private final ResourceLink links;

    private final Attributes attributes;

    private final Relationships relationships;

    public TopicInfoData(
            ResourceLink links,
            // TopicsMap topicsMap,
//            TopicListing topicListing,
            String topicName,
            Relationship topicListing
    ) {
        this.links = links;
        // check what attributes should be for listTopics
        // this.attributes = new Attributes(topicsMap);
        this.attributes = new Attributes(topicName);
        this.relationships = new Relationships(topicListing);
    }

    @JsonProperty("topic_name")
    public String getTopicName() {
        return attributes.getTopicName();
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

        TopicInfoData that = (TopicInfoData) o;
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
        // private final TopicsMap topicsMap;
        private final String topicName;

        public Attributes(String topicName) {
            // this.topicsMap = topicsMap;
            this.topicName = topicName;
        }

        @JsonProperty("topic_name")
        public String getTopicName() {
            return topicName;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            TopicInfoData.Attributes that = (TopicInfoData.Attributes) o;
            return Objects.equals(topicName, that.topicName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(topicName);
        }

        @Override
        public String toString() {
            return new StringJoiner(", ", TopicInfoData.Attributes.class.getSimpleName() + "[", "]")
                    .add("topicName='" + topicName + "'")
                    .toString();
        }
    }

    public static final class Relationships {
        private final Relationship topicListing;

        public Relationships (Relationship topicsMap) {
            this.topicListing = topicsMap;
        }

        @JsonProperty("topicListing")
        public Relationship getTopicListing() {
            return topicListing;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Relationships that = (Relationships) o;
            return Objects.equals(topicListing, that.topicListing);
        }

        @Override
        public int hashCode() {
            return Objects.hash(topicListing);
        }

        @Override
        public String toString() {
            return new StringJoiner(", ", Relationships.class.getSimpleName() + "[", "]")
                    .add("topicListing="+ topicListing)
                    .toString();
        }
    }
}
