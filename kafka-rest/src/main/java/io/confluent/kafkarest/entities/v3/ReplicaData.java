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
import io.confluent.kafkarest.entities.PartitionReplica;
import io.confluent.kafkarest.response.CrnFactory;
import io.confluent.kafkarest.response.UrlFactory;
import java.util.Objects;
import java.util.StringJoiner;

/**
 * A replica resource type.
 */
@JsonIgnoreProperties(value = {"type"}, allowGetters = true)
public final class ReplicaData {

  public static final String ELEMENT_TYPE = "replica";

  private final String id;

  private final ResourceLink links;

  private final Attributes attributes;

  private final Relationships relationships;

  public ReplicaData(
      String id,
      ResourceLink links,
      String clusterId,
      String topicName,
      Integer partitionId,
      Integer brokerId,
      Boolean isLeader,
      Boolean isInSync,
      Relationship broker) {
    this(id, links,
            new Attributes(clusterId, topicName, partitionId, brokerId, isLeader, isInSync),
            new Relationships(broker));
  }

  @JsonCreator
  public ReplicaData(
          @JsonProperty("id") String id,
          @JsonProperty("links") ResourceLink links,
          @JsonProperty("attributes") Attributes attributes,
          @JsonProperty("relationships") Relationships relationships
  ) {
    this.id = requireNonNull(id);
    this.links = requireNonNull(links);
    this.attributes = attributes;
    this.relationships = relationships;
  }

  public static ReplicaData create(
      CrnFactory crnFactory, UrlFactory urlFactory, PartitionReplica replica) {
    return new ReplicaData(
        createId(crnFactory, replica),
        new ResourceLink(createSelfLink(urlFactory, replica)),
        replica.getClusterId(),
        replica.getTopicName(),
        replica.getPartitionId(),
        replica.getBrokerId(),
        replica.isLeader(),
        replica.isInSync(),
        new Relationship(createBrokerLink(urlFactory, replica)));
  }

  private static String createId(CrnFactory crnFactory, PartitionReplica replica) {
    return crnFactory.create(
        ClusterData.ELEMENT_TYPE,
        replica.getClusterId(),
        TopicData.ELEMENT_TYPE,
        replica.getTopicName(),
        PartitionData.ELEMENT_TYPE,
        Integer.toString(replica.getPartitionId()),
        ReplicaData.ELEMENT_TYPE,
        Integer.toString(replica.getBrokerId()));
  }

  private static String createSelfLink(UrlFactory urlFactory, PartitionReplica replica) {
    return urlFactory.create(
        "v3",
        "clusters",
        replica.getClusterId(),
        "topics",
        replica.getTopicName(),
        "partitions",
        Integer.toString(replica.getPartitionId()),
        "replicas",
        Integer.toString(replica.getBrokerId()));
  }

  private static String createBrokerLink(UrlFactory urlFactory, PartitionReplica replica) {
    return urlFactory.create(
        "v3",
        "clusters",
        replica.getClusterId(),
        "brokers",
        Integer.toString(replica.getBrokerId()));
  }

  @JsonProperty("type")
  public String getType() {
    return "KafkaReplica";
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
    ReplicaData that = (ReplicaData) o;
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
    return new StringJoiner(", ", ReplicaData.class.getSimpleName() + "[", "]")
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

    private final Integer brokerId;

    private final Boolean isLeader;

    private final Boolean isInSync;

    @JsonCreator
    public Attributes(
        @JsonProperty("cluster_id") String clusterId,
        @JsonProperty("topic_name") String topicName,
        @JsonProperty("partition_id") Integer partitionId,
        @JsonProperty("broker_id") Integer brokerId,
        @JsonProperty("is_leader") Boolean isLeader,
        @JsonProperty("is_in_sync") Boolean isInSync
    ) {
      this.clusterId = requireNonNull(clusterId);
      this.topicName = requireNonNull(topicName);
      this.partitionId = requireNonNull(partitionId);
      this.brokerId = requireNonNull(brokerId);
      this.isLeader = requireNonNull(isLeader);
      this.isInSync = requireNonNull(isInSync);
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

    @JsonProperty("broker_id")
    public Integer getBrokerId() {
      return brokerId;
    }

    @JsonProperty("is_leader")
    public Boolean getLeader() {
      return isLeader;
    }

    @JsonProperty("is_in_sync")
    public Boolean getInSync() {
      return isInSync;
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
          && Objects.equals(brokerId, that.brokerId)
          && Objects.equals(isLeader, that.isLeader)
          && Objects.equals(isInSync, that.isInSync);
    }

    @Override
    public int hashCode() {
      return Objects.hash(clusterId, topicName, partitionId, brokerId, isLeader, isInSync);
    }

    @Override
    public String toString() {
      return new StringJoiner(", ", Attributes.class.getSimpleName() + "[", "]")
          .add("clusterId='" + clusterId + "'")
          .add("topicName='" + topicName + "'")
          .add("partitionId=" + partitionId)
          .add("brokerId=" + brokerId)
          .add("isLeader=" + isLeader)
          .add("isInSync=" + isInSync)
          .toString();
    }
  }

  public static final class Relationships {

    public final Relationship broker;

    @JsonCreator
    public Relationships(@JsonProperty("broker") Relationship broker) {
      this.broker = requireNonNull(broker);
    }

    @JsonProperty("broker")
    public Relationship getBroker() {
      return broker;
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
      return Objects.equals(broker, that.broker);
    }

    @Override
    public int hashCode() {
      return Objects.hash(broker);
    }

    @Override
    public String toString() {
      return new StringJoiner(", ", Relationships.class.getSimpleName() + "[", "]")
          .add("broker=" + broker)
          .toString();
    }
  }
}
