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
 * A broker resource type.
 */
public final class BrokerData {

  public static final String TYPE = "broker";

  private final String id;

  private final ResourceLink links;

  private final Attributes attributes;

  private final Relationships relationships;

  public BrokerData(
      String id,
      ResourceLink links,
      String clusterId,
      Integer brokerId,
      @Nullable String host,
      @Nullable Integer port,
      @Nullable String rack,
      Relationship configurations,
      Relationship partitionReplicas) {
    this.id = Objects.requireNonNull(id);
    this.links = Objects.requireNonNull(links);
    attributes = new Attributes(clusterId, brokerId, host, port, rack);
    relationships = new Relationships(configurations, partitionReplicas);
  }

  @JsonProperty("type")
  public String getType() {
    return TYPE;
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
    BrokerData that = (BrokerData) o;
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
    return new StringJoiner(", ", BrokerData.class.getSimpleName() + "[", "]")
        .add("id='" + id + "'")
        .add("links=" + links)
        .add("attributes=" + attributes)
        .add("relationships=" + relationships)
        .toString();
  }

  public static final class Attributes {

    private final String clusterId;

    private final Integer brokerId;

    @Nullable
    private final String host;

    @Nullable
    private final Integer port;

    @Nullable
    private final String rack;

    public Attributes(
        String clusterId,
        Integer brokerId,
        @Nullable String host,
        @Nullable Integer port,
        @Nullable String rack) {
      this.clusterId = Objects.requireNonNull(clusterId);
      this.brokerId = Objects.requireNonNull(brokerId);
      this.host = host;
      this.port = port;
      this.rack = rack;
    }

    @JsonProperty("cluster_id")
    public String getClusterId() {
      return clusterId;
    }

    @JsonProperty("broker_id")
    public Integer getBrokerId() {
      return brokerId;
    }

    @JsonProperty("host")
    @Nullable
    public String getHost() {
      return host;
    }

    @JsonProperty("port")
    @Nullable
    public Integer getPort() {
      return port;
    }

    @JsonProperty("rack")
    @Nullable
    public String getRack() {
      return rack;
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
          && Objects.equals(brokerId, that.brokerId)
          && Objects.equals(host, that.host)
          && Objects.equals(port, that.port)
          && Objects.equals(rack, that.rack);
    }

    @Override
    public int hashCode() {
      return Objects.hash(clusterId, brokerId, host, port, rack);
    }

    @Override
    public String toString() {
      return new StringJoiner(", ", Attributes.class.getSimpleName() + "[", "]")
          .add("clusterId='" + clusterId + "'")
          .add("brokerId=" + brokerId)
          .add("host='" + host + "'")
          .add("port=" + port)
          .add("rack='" + rack + "'")
          .toString();
    }
  }

  public static final class Relationships {

    private final Relationship configurations;

    private final Relationship partitionReplicas;

    public Relationships(Relationship configurations, Relationship partitionReplicas) {
      this.configurations = Objects.requireNonNull(configurations);
      this.partitionReplicas = Objects.requireNonNull(partitionReplicas);
    }

    @JsonProperty("configurations")
    public Relationship getConfigurations() {
      return configurations;
    }

    @JsonProperty("partition_replicas")
    public Relationship getPartitionReplicas() {
      return partitionReplicas;
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
          && Objects.equals(partitionReplicas, that.partitionReplicas);
    }

    @Override
    public int hashCode() {
      return Objects.hash(configurations, partitionReplicas);
    }

    @Override
    public String toString() {
      return new StringJoiner(", ", Relationships.class.getSimpleName() + "[", "]")
          .add("configurations=" + configurations)
          .add("partitionReplicas=" + partitionReplicas)
          .toString();
    }
  }
}
