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

package io.confluent.kafkarest.entities;

import static java.util.Objects.requireNonNull;

import com.google.auto.value.AutoValue;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nullable;

@AutoValue
public abstract class ConsumerGroupLag {

  ConsumerGroupLag() {
  }

  public abstract Long getMaxLag();

  public abstract Long getTotalLag();

  public abstract String getClusterId();

  public abstract String getConsumerGroupId();

  public abstract String getMaxLagClientId();

  public abstract String getMaxLagConsumerId();

  public abstract Optional<String> getMaxLagInstanceId();

  public abstract String getMaxLagTopicName();

  public abstract Integer getMaxLagPartitionId();

  public static Builder builder() {
    return new AutoValue_ConsumerGroupLag.Builder();
  }

  private static final class TopicOffsets {
    private final String topicName;

    private long maxLag = 0L;
    // private final Set<Offset> topicOffsets = new HashSet<>();

    private TopicOffsets(String topicName) {
      this.topicName = requireNonNull(topicName);
    }

    private void addOffset(Offset offset) {
      // if (topicOffsets.contains(offset)) {
      //   return;
      // }
      // topicOffsets.add(offset);
      maxLag = Math.max(maxLag, offset.getLag());
    }

    // private Set<Offset> getTopicOffsets() {
    //   return topicOffsets;
    // }

    private long getMaxLag() {
      return maxLag;
    }
  }

  @AutoValue
  abstract static class Offset {

    abstract String getTopicName();

    abstract String getConsumerId();

    abstract String getClientId();

    abstract int getPartitionId();

    abstract long getCurrentOffset();

    abstract long getEndOffset();

    private long getLag() {
      return getEndOffset() - getCurrentOffset();
    }

    private static Builder builder() {
      return new AutoValue_ConsumerGroupLag_Offset.Builder();
    }

    @AutoValue.Builder
    abstract static class Builder {

      abstract Builder setTopicName(String topicName);

      abstract Builder setConsumerId(String consumerId);

      abstract Builder setClientId(String clientId);

      abstract Builder setPartitionId(int partitionId);

      abstract Builder setCurrentOffset(long currentOffset);

      abstract Builder setEndOffset(long endOffset);

      abstract Offset build();
    }
  }

  @AutoValue.Builder
  public abstract static class Builder {

    private long maxLag = 0;
    private long totalLag = 0;

    private final Map<String, TopicOffsets> consumerGroupOffsets = new HashMap<>();
    private final Set<String> consumers = new HashSet<>();

    Builder() {
    }

    public void addOffset(
        String topicName,
        String consumerId,
        String clientId,
        int partitionId,
        long currentOffset,
        long endOffset
    ) {
      TopicOffsets topicOffsets =
          consumerGroupOffsets.computeIfAbsent(topicName, unused -> new TopicOffsets(topicName));

      Offset offset =
          Offset.builder()
              .setTopicName(topicName)
              .setConsumerId(consumerId)
              .setClientId(clientId)
              .setPartitionId(partitionId)
              .setCurrentOffset(currentOffset)
              .setEndOffset(endOffset)
              .build();

      topicOffsets.addOffset(offset);

      if (maxLag < offset.getLag()) {
        maxLag = offset.getLag();
        setMaxLag(maxLag);
        setMaxLagClientId(clientId);
        setMaxLagConsumerId(consumerId);
        setMaxLagTopicName(topicName);
        setMaxLagPartitionId(partitionId);
      }

      totalLag += offset.getLag();
      setTotalLag(totalLag);

      // MMA-3352: not adding consumers that are empty. this likely happens when a consumer group
      //           has no active members. however we are calling addOffset to fix the issue of
      //           lag data not showing up for groups w/o members like in the case of replicator
      if (consumerId != null && !consumerId.isEmpty()) {
        consumers.add(consumerId);
      }
    }

    public abstract Builder setClusterId(String clusterId);

    public abstract Builder setConsumerGroupId(String consumerGroupId);

    public abstract Builder setMaxLag(Long maxLag);

    public abstract Builder setTotalLag(Long totalLag);

    public abstract Builder setMaxLagClientId(String maxLagClientId);

    public abstract Builder setMaxLagConsumerId(String maxLagConsumerId);

    public abstract Builder setMaxLagInstanceId(@Nullable String maxLagInstanceId);

    public abstract Builder setMaxLagTopicName(String maxLagTopicName);

    public abstract Builder setMaxLagPartitionId(Integer maxLagPartitionId);

    public abstract ConsumerGroupLag build();
  }
}
