package io.confluent.kafkarest.entities.v3;


import com.fasterxml.jackson.annotation.JsonProperty;
import edu.umd.cs.findbugs.annotations.Nullable;

import java.util.Objects;
import java.util.StringJoiner;

/**
 * A KafkaCluster resource type
 */
public class TopicData {

    private final ResourceLink links;

    private final Attributes attributes;

    private final Relationships relationships;

    public TopicData(
            ResourceLink links,
            String topic,
            Relationship isInternal,
            Relationship replicationFactor,
            Relationship configuration,
            Relationship partitions
    ) {
        this.links = links;
        this.attributes = new Attributes(topic);
        this.relationships = new Relationships(isInternal,
                replicationFactor,
                configuration,
                partitions);
    }

    @JsonProperty("topic")
    public String topic() {
        return attributes.getTopic();
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

        TopicData that = (TopicData) o;
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
        private final String topic;

        public Attributes(String topic) {
            this.topic = Objects.requireNonNull(topic);
        }

        @JsonProperty("topic")
        public String getTopic() {
            return topic;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            TopicData.Attributes that = (TopicData.Attributes) o;
            return Objects.equals(topic, that.topic);
        }

        @Override
        public int hashCode() {
            return Objects.hash(topic);
        }

        @Override
        public String toString() {
            return new StringJoiner(", ", ClusterData.Attributes.class.getSimpleName() + "[", "]")
                    .add("topic='" + topic + "'")
                    .toString();
        }
    }

    public static final class Relationships {

        private final Relationship isInternal;
        private final Relationship replicationFactor;
        private final Relationship configuration;
        private final Relationship partitions;

        // todo ask if this is to be added
        // private final Relationship clusterId;

        //todo check this later
        public Relationships(Relationship isInternal,
                             Relationship replicationFactor,
                             Relationship configuration,
                             Relationship partitions) {
            this.isInternal = isInternal;
            this.replicationFactor = replicationFactor;
            this.configuration = configuration;
            this.partitions = partitions;
        }

        @JsonProperty("isInternal")
        public Relationship getIsInternal() {
            return isInternal;
        }

        @JsonProperty("replicationFactor")
        public Relationship getReplicationFactor() {
            return replicationFactor;
        }

        @JsonProperty("configuration")
        public Relationship getConfiguration() {
            return configuration;
        }

        @JsonProperty("partitions")
        public Relationship getPartitions() {
            return partitions;
        }

        public boolean equals(Object o) {
            if(this == o) {
                return true;
            }

            if(o == null || getClass() != o.getClass()) {
                return false;
            }

            Relationships that = (Relationships) o;
            return Objects.equals(isInternal, that.isInternal)
                    && Objects.equals(replicationFactor, that.replicationFactor)
                    && Objects.equals(configuration, that.configuration)
                    && Objects.equals(partitions, that.partitions);

        }

        public int hashCode() {
            return Objects.hash(isInternal,
                    replicationFactor,
                    configuration,
                    partitions);
        }

        public String toString() {
            return new StringJoiner(", ", Relationships.class.getSimpleName() + "[", "]")
                    .add("isInternal=" + isInternal)
                    .add("replicationFactor=" + replicationFactor)
                    .add("configuration="+configuration)
                    .add("partitions=" + partitions)
                    .toString();
        }
    }
}
