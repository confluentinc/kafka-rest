/*
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.kafkarest.entities;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class TopicPartitionEntity {

  private final String consumerId;
  private final String consumerIp;
  private final String topicName;
  private final Integer partitionId;
  private final Long currentOffset;
  private final Long lag;
  private final Long endOffset;

  @JsonCreator
  public TopicPartitionEntity(@JsonProperty("consumerId") String consumerId,
                              @JsonProperty("consumerIp") String consumerIp,
                              @JsonProperty("topicName") String topicName,
                              @JsonProperty("partitionId") Integer partitionId,
                              @JsonProperty("currentOffset") Long currentOffset,
                              @JsonProperty("lag") Long lag,
                              @JsonProperty("endOffset") Long endOffset) {
    this.consumerId = consumerId;
    this.consumerIp = consumerIp;
    this.topicName = topicName;
    this.partitionId = partitionId;
    this.currentOffset = currentOffset;
    this.lag = lag;
    this.endOffset = endOffset;
  }

  @JsonProperty
  public String getConsumerId() {
    return consumerId;
  }

  @JsonProperty
  public String getConsumerIp() {
    return consumerIp;
  }

  @JsonProperty
  public String getTopicName() {
    return topicName;
  }

  @JsonProperty
  public Integer getPartitionId() {
    return partitionId;
  }

  @JsonProperty
  public Long getCurrentOffset() {
    return currentOffset;
  }

  @JsonProperty
  public Long getLag() {
    return lag;
  }

  @JsonProperty
  public Long getEndOffset() {
    return endOffset;
  }

  @Override
  public String toString() {
    return "TopicPartitionEntity{"
            + "consumerId='" + consumerId + '\''
            + ", consumerIp='" + consumerIp + '\''
            + ", topicName='" + topicName + '\''
            + ", partitionId=" + partitionId
            + ", currentOffset=" + currentOffset
            + ", lag=" + lag
            + ", endOffset=" + endOffset
            + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    TopicPartitionEntity that = (TopicPartitionEntity) o;

    return consumerId.equals(that.consumerId)
            && consumerIp.equals(that.consumerIp)
            && topicName.equals(that.topicName)
            && partitionId.equals(that.partitionId)
            && currentOffset.equals(that.currentOffset)
            && lag.equals(that.lag)
            && endOffset.equals(that.endOffset);
  }

  @Override
  public int hashCode() {
    int result = consumerId.hashCode();
    result = 31 * result + consumerIp.hashCode();
    result = 31 * result + topicName.hashCode();
    result = 31 * result + partitionId.hashCode();
    result = 31 * result + currentOffset.hashCode();
    result = 31 * result + lag.hashCode();
    result = 31 * result + endOffset.hashCode();
    return result;
  }
}
