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
import io.confluent.kafkarest.entities.Broker;
import java.util.Optional;
import javax.annotation.Nullable;

@AutoValue
public abstract class BrokerData extends Resource {

  BrokerData() {
  }

  @JsonProperty("cluster_id")
  public abstract String getClusterId();

  @JsonProperty("broker_id")
  public abstract int getBrokerId();

  @JsonProperty("host")
  public abstract Optional<String> getHost();

  @JsonProperty("port")
  public abstract Optional<Integer> getPort();

  @JsonProperty("rack")
  public abstract Optional<String> getRack();

  @JsonProperty("configs")
  public abstract Relationship getConfigs();

  @JsonProperty("partition_replicas")
  public abstract Relationship getPartitionReplicas();

  public static Builder builder() {
    return new AutoValue_BrokerData.Builder().setKind("KafkaBroker");
  }

  public static Builder fromBroker(Broker broker) {
    return builder()
        .setClusterId(broker.getClusterId())
        .setBrokerId(broker.getBrokerId())
        .setHost(broker.getHost())
        .setPort(broker.getPort()).setRack(broker.getRack());
  }

  @JsonCreator
  static BrokerData fromJson(
      @JsonProperty("kind") String kind,
      @JsonProperty("metadata") Metadata metadata,
      @JsonProperty("cluster_id") String clusterId,
      @JsonProperty("broker_id") int brokerId,
      @JsonProperty("host") @Nullable String host,
      @JsonProperty("port") @Nullable Integer port,
      @JsonProperty("rack") @Nullable String rack,
      @JsonProperty("configs") Relationship configs,
      @JsonProperty("partition_replicas") Relationship partitionReplicas
  ) {
    return builder()
        .setKind(kind)
        .setMetadata(metadata)
        .setClusterId(clusterId)
        .setBrokerId(brokerId)
        .setHost(host)
        .setPort(port)
        .setRack(rack)
        .setConfigs(configs)
        .setPartitionReplicas(partitionReplicas)
        .build();
  }

  @AutoValue.Builder
  public abstract static class Builder extends Resource.Builder<Builder> {

    Builder() {
    }

    public abstract Builder setClusterId(String clusterId);

    public abstract Builder setBrokerId(int brokerId);

    public abstract Builder setHost(@Nullable String host);

    public abstract Builder setPort(@Nullable Integer port);

    public abstract Builder setRack(@Nullable String rack);

    public abstract Builder setConfigs(Relationship configs);

    public abstract Builder setPartitionReplicas(Relationship partitionReplicas);

    public abstract BrokerData build();
  }
}
