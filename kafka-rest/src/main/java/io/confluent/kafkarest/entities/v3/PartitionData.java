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

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;
import java.util.StringJoiner;
import javax.annotation.Nullable;

/**
 * A KafkaPartition resource type.
 */
public final class PartitionData {

  private final ResourceLink links;

  private final Attributes attributes;

  private final Relationships relationships;

  public PartitionData(
      ResourceLink links,
      String clusterId,
      String topicName,
      Integer partitionId,
      @Nullable Relationship leader,
      Relationship replicas
  ) {
    this.links = Objects.requireNonNull(links);
    attributes = new Attributes(clusterId, topicName, partitionId);
    relationships = new Relationships(leader, replicas);
  }

  @JsonProperty("type")
  public String getType() {
    return "KafkaPartition";
  }

  @JsonProperty("id")
  public String getId() {
    return String.format(
        "%s/%s/%s",
        attributes.getClusterId(),
        attributes.getTopicName(),
        attributes.getPartitionId());
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
    PartitionData that = (PartitionData) o;
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
    return new StringJoiner(", ", PartitionData.class.getSimpleName() + "[", "]")
        .add("links=" + links)
        .add("attributes=" + attributes)
        .add("relationships=" + relationships)
        .toString();
  }

  private static final class Attributes {

    private final String clusterId;

    private final String topicName;

    private final Integer partitionId;

    private Attributes(
        String clusterId,
        String topicName,
        Integer partitionId
    ) {
      this.clusterId = Objects.requireNonNull(clusterId);
      this.topicName = Objects.requireNonNull(topicName);
      this.partitionId = Objects.requireNonNull(partitionId);
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
          && Objects.equals(partitionId, that.partitionId);
    }

    @Override
    public int hashCode() {
      return Objects.hash(clusterId, topicName, partitionId);
    }

    @Override
    public String toString() {
      return new StringJoiner(", ", Attributes.class.getSimpleName() + "[", "]")
          .add("clusterId='" + clusterId + "'")
          .add("topicName='" + topicName + "'")
          .add("partitionId=" + partitionId)
          .toString();
    }
  }

  private static final class Relationships {

    @Nullable
    private final Relationship leader;

    private final Relationship replicas;

    private Relationships(@Nullable Relationship leader, Relationship replicas) {
      this.leader = leader;
      this.replicas = Objects.requireNonNull(replicas);
    }

    @JsonProperty("leader")
    @Nullable
    public Relationship getLeader() {
      return leader;
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
      return Objects.equals(leader, that.leader) && Objects.equals(replicas, that.replicas);
    }

    @Override
    public int hashCode() {
      return Objects.hash(leader, replicas);
    }

    @Override
    public String toString() {
      return new StringJoiner(", ", Relationships.class.getSimpleName() + "[", "]")
          .add("leader=" + leader)
          .add("replicas=" + replicas)
          .toString();
    }
  }
}
