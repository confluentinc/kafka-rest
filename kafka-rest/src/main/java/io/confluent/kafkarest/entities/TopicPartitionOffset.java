/**
 * Copyright 2015 Confluent Inc.
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
 **/

package io.confluent.kafkarest.entities;

import com.fasterxml.jackson.annotation.JsonProperty;

import org.hibernate.validator.constraints.NotEmpty;

import javax.validation.constraints.Min;

public class TopicPartitionOffset {

  @NotEmpty
  private String topic;
  @Min(0)
  private int partition;
  @Min(0)
  private long consumed;
  @Min(0)
  private long committed;

  public TopicPartitionOffset(
      @JsonProperty("topic") String topic,
      @JsonProperty("partition") int partition,
      @JsonProperty("consumed") long consumed,
      @JsonProperty("committed") long committed
  ) {
    this.topic = topic;
    this.partition = partition;
    this.consumed = consumed;
    this.committed = committed;
  }

  @JsonProperty
  public String getTopic() {
    return topic;
  }

  @JsonProperty
  public void setTopic(String topic) {
    this.topic = topic;
  }

  @JsonProperty
  public int getPartition() {
    return partition;
  }

  @JsonProperty
  public void setPartition(int partition) {
    this.partition = partition;
  }

  @JsonProperty
  public long getConsumed() {
    return consumed;
  }

  @JsonProperty
  public void setConsumed(long consumed) {
    this.consumed = consumed;
  }

  @JsonProperty
  public long getCommitted() {
    return committed;
  }

  @JsonProperty
  public void setCommitted(long committed) {
    this.committed = committed;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    TopicPartitionOffset that = (TopicPartitionOffset) o;

    if (committed != that.committed) {
      return false;
    }
    if (consumed != that.consumed) {
      return false;
    }
    if (partition != that.partition) {
      return false;
    }
    if (topic != null ? !topic.equals(that.topic) : that.topic != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int result = topic != null ? topic.hashCode() : 0;
    result = 31 * result + partition;
    result = 31 * result + (int) (consumed ^ (consumed >>> 32));
    result = 31 * result + (int) (committed ^ (committed >>> 32));
    return result;
  }
}
