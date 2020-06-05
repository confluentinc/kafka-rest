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
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.auto.value.AutoValue;
import io.confluent.kafkarest.entities.Cluster;
import java.util.Optional;
import javax.annotation.Nullable;

@AutoValue
public abstract class ClusterData extends Resource {

  public static final String ELEMENT_TYPE = "kafka";

  ClusterData() {
  }

  @JsonProperty("cluster_id")
  public abstract String getClusterId();

  @JsonProperty("controller")
  public abstract Optional<Relationship> getController();

  @JsonProperty("brokers")
  public abstract Relationship getBrokers();

  @JsonProperty("topics")
  public abstract Relationship getTopics();

  @JsonProperty("broker_configs")
  public abstract Relationship getBrokerConfigs();

  @JsonProperty("topic_configs")
  public abstract Relationship getTopicConfigs();

  public static Builder builder() {
    return new AutoValue_ClusterData.Builder().setKind("KafkaCluster");
  }

  public static Builder fromCluster(Cluster cluster) {
    return builder().setClusterId(cluster.getClusterId());
  }

  @JsonCreator
  static ClusterData fromJson(
      @JsonProperty("kind") String kind,
      @JsonProperty("metadata") Metadata metadata,
      @JsonProperty("cluster_id") String clusterId,
      @JsonProperty("controller") @Nullable Relationship controller,
      @JsonProperty("brokers") Relationship brokers,
      @JsonProperty("topics") Relationship topics,
      @JsonProperty("broker_configs") Relationship brokerConfigs,
      @JsonProperty("topic_configs") Relationship topicConfigs
  ) {
    return builder()
        .setKind(kind)
        .setMetadata(metadata)
        .setClusterId(clusterId)
        .setController(controller)
        .setBrokers(brokers)
        .setTopics(topics)
        .setBrokerConfigs(brokerConfigs)
        .setTopicConfigs(topicConfigs)
        .build();
  }

  @AutoValue.Builder
  public abstract static class Builder extends Resource.Builder<Builder> {

    Builder() {
    }

    public abstract Builder setClusterId(String clusterId);

    public abstract Builder setController(@Nullable Relationship controller);

    public abstract Builder setBrokers(Relationship brokers);

    public abstract Builder setTopics(Relationship topics);

    public abstract Builder setBrokerConfigs(Relationship brokerConfigs);

    public abstract Builder setTopicConfigs(Relationship topicConfigs);

    public abstract ClusterData build();
  }
}
