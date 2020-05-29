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


import static java.util.Objects.requireNonNull;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.Objects;
import java.util.StringJoiner;

/**
 * A reassignment resource type.
 */
@JsonIgnoreProperties(value = {"type"}, allowGetters = true)
public final class ReassignmentData {

  public static final String ELEMENT_TYPE = "reassignment";

  private final String id;

  private final ResourceLink links;

  private final Attributes attributes;

  private final Relationships relationships;

  public ReassignmentData(
      String id,
      ResourceLink links,
      String clusterId,
      String topicName,
      Integer partitionId,
      List<Integer> replicas,
      List<Integer> addingReplicas,
      List<Integer> removingReplicas,
      Relationship replicasLink
  ) {
    this(id, links,
        new Attributes(
        clusterId, topicName, partitionId, replicas, addingReplicas, removingReplicas),
        new Relationships(replicasLink));
  }

  @JsonCreator
  public ReassignmentData(
      @JsonProperty("id") String id,
      @JsonProperty("links") ResourceLink links,
      @JsonProperty("attributes") Attributes attributes,
      @JsonProperty("relationships") Relationships relationships
  ) {
    this.id = id;
    this.links = links;
    this.attributes = attributes;
    this.relationships = relationships;
  }

  @JsonProperty("type")
  public String getType() {
    return "KafkaReassignment";
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
    ReassignmentData that = (ReassignmentData) o;
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
    return new StringJoiner(", ", ReassignmentData.class.getSimpleName() + "[", "]")
        .add("id='" + id + "'")
        .add("links=" + links)
        .add("attributes=" + attributes)
        .add("relationships=" + relationships)
        .toString();
  }

  public static final class Attributes {

    private final String clusterId;

    private final String topicName;

    private final Integer partitionId;

    private final List<Integer> replicas;

    private final List<Integer> addingReplicas;

    private final List<Integer> removingReplicas;

    @JsonCreator
    public Attributes(
        @JsonProperty("cluster_id") String clusterId,
        @JsonProperty("topic_name") String topicName,
        @JsonProperty("partition_id") Integer partitionId,
        @JsonProperty("replicas") List<Integer> replicas,
        @JsonProperty("adding_replicas") List<Integer> addingReplicas,
        @JsonProperty("removing_replicas") List<Integer> removingReplicas
    ) {
      this.clusterId = requireNonNull(clusterId);
      this.topicName = requireNonNull(topicName);
      this.partitionId = requireNonNull(partitionId);
      this.replicas = requireNonNull(replicas);
      this.addingReplicas = requireNonNull(addingReplicas);
      this.removingReplicas = requireNonNull(removingReplicas);
    }

    @JsonProperty("cluster_id")
    public String getClusterId() {
      return clusterId;
    }

    @JsonProperty("topic_name")
    public String getTopicName() {
      return topicName;
    }

    @JsonProperty("partition_id")
    public Integer getPartitionId() {
      return partitionId;
    }

    @JsonProperty("replicas")
    public List<Integer> getReplicas() {
      return replicas;
    }

    @JsonProperty("adding_replicas")
    public List<Integer> getAddingReplicas() {
      return addingReplicas;
    }

    @JsonProperty("removing_replicas")
    public List<Integer> getRemovingReplicas() {
      return removingReplicas;
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
      return Objects.equals(clusterId, that.clusterId)
          && Objects.equals(topicName, that.topicName)
          && Objects.equals(partitionId, that.partitionId)
          && Objects.equals(replicas, that.replicas)
          && Objects.equals(addingReplicas, that.addingReplicas)
          && Objects.equals(removingReplicas, that.removingReplicas);
    }

    @Override
    public int hashCode() {
      return Objects
          .hash(clusterId, topicName, partitionId, replicas, addingReplicas, removingReplicas);
    }

    @Override
    public String toString() {
      return new StringJoiner(", ", Attributes.class.getSimpleName() + "[", "]")
          .add("clusterId='" + clusterId + "'")
          .add("topicName='" + topicName + "'")
          .add("partitionId=" + partitionId)
          .add("replicas=" + replicas)
          .add("addingReplicas=" + addingReplicas)
          .add("removingReplicas=" + removingReplicas)
          .toString();
    }
  }

  private static final class Relationships {

    private final Relationship replicas;

    private Relationships(@JsonProperty("replicas") Relationship replicas) {
      this.replicas = replicas;
    }

    @JsonProperty("replicas")
    public Relationship getReplicas() {
      return replicas;
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
      return Objects.equals(replicas, that.replicas);
    }

    @Override
    public int hashCode() {
      return Objects.hash(replicas);
    }

    @Override
    public String toString() {
      return new StringJoiner(", ", Relationships.class.getSimpleName() + "[", "]")
          .add("replicas=" + replicas)
          .toString();
    }
  }
}
