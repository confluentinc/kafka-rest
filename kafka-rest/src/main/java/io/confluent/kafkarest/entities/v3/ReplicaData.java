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
import io.confluent.kafkarest.entities.PartitionReplica;

@AutoValue
public abstract class ReplicaData extends Resource {

  ReplicaData() {
  }

  @JsonProperty("cluster_id")
  public abstract String getClusterId();

  @JsonProperty("topic_name")
  public abstract String getTopicName();

  @JsonProperty("partition_id")
  public abstract int getPartitionId();

  @JsonProperty("broker_id")
  public abstract int getBrokerId();

  @JsonProperty("is_leader")
  public abstract boolean isLeader();

  @JsonProperty("is_in_sync")
  public abstract boolean isInSync();

  @JsonProperty("broker")
  public abstract Relationship getBroker();

  public static Builder builder() {
    return new AutoValue_ReplicaData.Builder().setKind("KafkaReplica");
  }

  public static Builder fromPartitionReplica(PartitionReplica replica) {
    return builder()
        .setClusterId(replica.getClusterId())
        .setTopicName(replica.getTopicName())
        .setPartitionId(replica.getPartitionId())
        .setBrokerId(replica.getBrokerId())
        .setLeader(replica.isLeader())
        .setInSync(replica.isInSync());
  }

  @JsonCreator
  static ReplicaData fromJson(
      @JsonProperty("kind") String kind,
      @JsonProperty("metadata") Metadata metadata,
      @JsonProperty("cluster_id") String clusterId,
      @JsonProperty("topic_name") String topicName,
      @JsonProperty("partition_id") int partitionId,
      @JsonProperty("broker_id") int brokerId,
      @JsonProperty("is_leader") boolean isLeader,
      @JsonProperty("is_in_sync") boolean isInSync,
      @JsonProperty("broker") Relationship broker
  ) {
    return builder()
        .setKind(kind)
        .setMetadata(metadata)
        .setClusterId(clusterId)
        .setTopicName(topicName)
        .setPartitionId(partitionId)
        .setBrokerId(brokerId)
        .setLeader(isLeader)
        .setInSync(isInSync)
        .setBroker(broker)
        .build();
  }

  @AutoValue.Builder
  public abstract static class Builder extends Resource.Builder<Builder> {

    Builder() {
    }

    public abstract Builder setClusterId(String clusterId);

    public abstract Builder setTopicName(String topicName);

    public abstract Builder setPartitionId(int partitionId);

    public abstract Builder setBrokerId(int brokerId);

    public abstract Builder setLeader(boolean isLeader);

    public abstract Builder setInSync(boolean isInSync);

    public abstract Builder setBroker(Relationship broker);

    public abstract ReplicaData build();
  }
}
