package io.confluent.kafkarest.entities.v3;


import com.fasterxml.jackson.annotation.JsonProperty;

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
      String topicName,
      String clusterId,
      boolean isInternal,
      int replicationFactor,
      Relationship configuration,
      Relationship partitions
  ) {
    this.links = links;
    this.attributes = new Attributes(topicName, clusterId, isInternal, replicationFactor);
    this.relationships = new Relationships(
        configuration,
        partitions);
  }

  @JsonProperty("topic")
  public String topic() {
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

    private final String topicName;
    private final String clusterId;
    private final boolean isInternal;
    private final int replicationFactor;

    public Attributes(String topicName, String clusterId, boolean isInternal,
        int replicationFactor) {
      this.topicName = topicName;
      this.clusterId = Objects.requireNonNull(clusterId);
      this.isInternal = Objects.requireNonNull(isInternal);
      this.replicationFactor = Objects.requireNonNull(replicationFactor);
    }

    @JsonProperty("topic")
    public String getTopicName() {
      return topicName;
    }

    @JsonProperty("clusterId")
    public String getClusterId() {
      return clusterId;
    }

    @JsonProperty("isInternal")
    public boolean isInternal() {
      return isInternal;
    }

    @JsonProperty("replicationFactor")
    public int getReplicationFactor() {
      return replicationFactor;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      Attributes that = (Attributes) o;
      return isInternal == that.isInternal &&
          replicationFactor == that.replicationFactor &&
          Objects.equals(topicName, that.topicName) &&
          Objects.equals(clusterId, that.clusterId);
    }

    @Override
    public int hashCode() {
      return Objects.hash(topicName, clusterId, isInternal, replicationFactor);
    }

    @Override
    public String toString() {
      return new StringJoiner(", ", ClusterData.Attributes.class.getSimpleName() + "[", "]")
          .add("topic='" + topicName + "'")
          .add("clusterId='" + clusterId + "'")
          .add("isInternal='" + isInternal + "'")
          .add("replicationFactor='" + replicationFactor + "'")
          .toString();
    }
  }

  public static final class Relationships {

    private final Relationship configuration;
    private final Relationship partitions;

    // todo ask if this is to be added
    // private final Relationship clusterId;

    //todo check this later
    public Relationships(Relationship configuration,
        Relationship partitions) {
      this.configuration = configuration;
      this.partitions = partitions;
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
      if (this == o) {
        return true;
      }

      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      Relationships that = (Relationships) o;
      return Objects.equals(configuration, that.configuration)
          && Objects.equals(partitions, that.partitions);

    }

    public int hashCode() {
      return Objects.hash(configuration, partitions);
    }

    public String toString() {
      return new StringJoiner(", ", Relationships.class.getSimpleName() + "[", "]")
          .add("configuration=" + configuration)
          .add("partitions=" + partitions)
          .toString();
    }
  }
}
