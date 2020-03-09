/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.kafkarest.entities.v3;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;
import java.util.StringJoiner;

/**
 * A KafkaTopic resource type
 */
public final class TopicData {

  private final ResourceLink links;

  private final Attributes attributes;

  private final Relationships relationships;

  public TopicData(
      ResourceLink links,
      String clusterId,
      String topicName,
      boolean isInternal,
      int replicationFactor,
      Relationship configurations,
      Relationship partitions
  ) {
    this.links = Objects.requireNonNull(links);
    attributes = new Attributes(clusterId, topicName, isInternal, replicationFactor);
    relationships = new Relationships(configurations, partitions);
  }

  @JsonProperty("type")
  public String getType() {
    return "KafkaTopic";
  }

  @JsonProperty("id")
  public String getId() {
    return String.format("%s/%s", attributes.getClusterId(), attributes.getTopicName());
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
    TopicData topicData = (TopicData) o;
    return Objects.equals(links, topicData.links)
        && Objects.equals(attributes, topicData.attributes)
        && Objects.equals(relationships, topicData.relationships);
  }

  @Override
  public int hashCode() {
    return Objects.hash(links, attributes, relationships);
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", TopicData.class.getSimpleName() + "[", "]")
        .add("links=" + links)
        .add("attributes=" + attributes)
        .add("relationships=" + relationships)
        .toString();
  }

  public static final class Attributes {

    private final String clusterId;

    private final String topicName;

    private final boolean isInternal;

    private final int replicationFactor;

    public Attributes(
        String clusterId, String topicName, boolean isInternal, int replicationFactor) {
      this.clusterId = Objects.requireNonNull(clusterId);
      this.topicName = Objects.requireNonNull(topicName);
      this.isInternal = isInternal;
      this.replicationFactor = replicationFactor;
    }

    @JsonProperty("cluster_id")
    public String getClusterId() {
      return clusterId;
    }

    @JsonProperty("topic_name")
    public String getTopicName() {
      return topicName;
    }

    @JsonProperty("is_internal")
    public boolean isInternal() {
      return isInternal;
    }

    @JsonProperty("replication_factor")
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
      return isInternal == that.isInternal
          && replicationFactor == that.replicationFactor
          && Objects.equals(clusterId, that.clusterId)
          && Objects.equals(topicName, that.topicName);
    }

    @Override
    public int hashCode() {
      return Objects.hash(clusterId, topicName, isInternal, replicationFactor);
    }

    @Override
    public String toString() {
      return new StringJoiner(", ", Attributes.class.getSimpleName() + "[", "]")
          .add("clusterId='" + clusterId + "'")
          .add("topicName='" + topicName + "'")
          .add("isInternal=" + isInternal)
          .add("replicationFactor=" + replicationFactor)
          .toString();
    }
  }

  public static final class Relationships {

    private final Relationship configurations;

    private final Relationship partitions;

    public Relationships(Relationship configurations, Relationship partitions) {
      this.configurations = configurations;
      this.partitions = partitions;
    }

    @JsonProperty("configurations")
    public Relationship getConfigurations() {
      return configurations;
    }

    @JsonProperty("partitions")
    public Relationship getPartitions() {
      return partitions;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      Relationships that = (Relationships) o;
      return Objects.equals(configurations, that.configurations)
          && Objects.equals(partitions, that.partitions);
    }

    @Override
    public int hashCode() {
      return Objects.hash(configurations, partitions);
    }

    @Override
    public String toString() {
      return new StringJoiner(", ", Relationships.class.getSimpleName() + "[", "]")
          .add("configurations=" + configurations)
          .add("partitions=" + partitions)
          .toString();
    }
  }
}
