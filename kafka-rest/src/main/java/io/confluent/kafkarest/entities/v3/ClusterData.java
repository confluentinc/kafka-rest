/*
 * Copyright 2020 Confluent Inc.
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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;
import java.util.StringJoiner;
import javax.annotation.Nullable;

/**
 * A kafka resource type.
 */
public final class ClusterData {

  public static final String ELEMENT_TYPE = "kafka";

  private final String id;

  private final ResourceLink links;

  private final Attributes attributes;

  private final Relationships relationships;


  public ClusterData(
      String id,
      ResourceLink links,
      String clusterId,
      Relationship controller,
      Relationship brokers,
      Relationship topics
  ) {
    this(id, links,
            new Attributes(clusterId),
            new Relationships(controller, brokers, topics));
  }

  @JsonCreator
  public ClusterData(
          @JsonProperty("type") String id,
          @JsonProperty("links") ResourceLink links,
          @JsonProperty("cluster_id") Attributes attributes,
          @JsonProperty("relationships") Relationships relationships
  ) {
    this.id = Objects.requireNonNull(id);
    this.links = Objects.requireNonNull(links);
    this.attributes = attributes;
    this.relationships = relationships;
  }

  @JsonProperty("type")
  public String getType() {
    return "KafkaCluster";
  }

  @JsonProperty("id")
  public String getId() {
    return id;
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
    ClusterData that = (ClusterData) o;
    return Objects.equals(id, that.id)
        && Objects.equals(links, that.links)
        && Objects.equals(attributes, that.attributes)
        && Objects.equals(relationships, that.relationships);

  }

  @Override
  public int hashCode() {
    return Objects.hash(id, links, attributes, relationships);
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", ClusterData.class.getSimpleName() + "[", "]")
        .add("id='" + id + "'")
        .add("links=" + links)
        .add("attributes=" + attributes)
        .add("relationships=" + relationships)
        .toString();
  }

  public static final class Attributes {

    private final String clusterId;

    @JsonCreator
    public Attributes(@JsonProperty("cluster_id") String clusterId) {
      this.clusterId = Objects.requireNonNull(clusterId);
    }

    @JsonProperty("cluster_id")
    public String getClusterId() {
      return clusterId;
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
      return Objects.equals(clusterId, that.clusterId);
    }

    @Override
    public int hashCode() {
      return Objects.hash(clusterId);
    }

    @Override
    public String toString() {
      return new StringJoiner(", ", Attributes.class.getSimpleName() + "[", "]")
          .add("clusterId='" + clusterId + "'")
          .toString();
    }
  }

  public static final class Relationships {

    @Nullable
    private final Relationship controller;

    private final Relationship brokers;

    private final Relationship topics;

    @JsonCreator
    public Relationships(
        @JsonProperty("controller") @Nullable Relationship controller,
        @JsonProperty("brokers") Relationship brokers,
        @JsonProperty("topics") Relationship topics) {
      this.controller = controller;
      this.brokers = Objects.requireNonNull(brokers);
      this.topics = Objects.requireNonNull(topics);
    }

    @JsonProperty("controller")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Nullable
    public Relationship getController() {
      return controller;
    }

    @JsonProperty("brokers")
    public Relationship getBrokers() {
      return brokers;
    }

    @JsonProperty("topics")
    public Relationship getTopics() {
      return topics;
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
      return Objects.equals(controller, that.controller)
          && Objects.equals(brokers, that.brokers)
          && Objects.equals(topics, that.topics);
    }

    @Override
    public int hashCode() {
      return Objects.hash(controller, brokers, topics);
    }

    @Override
    public String toString() {
      return new StringJoiner(", ", Relationships.class.getSimpleName() + "[", "]")
          .add("controller=" + controller)
          .add("brokers=" + brokers)
          .add("topics=" + topics)
          .toString();
    }
  }
}
