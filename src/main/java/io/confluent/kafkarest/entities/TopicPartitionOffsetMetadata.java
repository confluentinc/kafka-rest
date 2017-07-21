/**
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
 **/

package io.confluent.kafkarest.entities;

import com.fasterxml.jackson.annotation.JsonProperty;

import org.hibernate.validator.constraints.NotEmpty;

import java.util.Objects;
import javax.validation.constraints.Min;

public class TopicPartitionOffsetMetadata {

  @NotEmpty
  private String topic;
  @Min(0)
  private int partition;
  @Min(0)
  private long offset;

  private String metadata;

  public TopicPartitionOffsetMetadata(
      @JsonProperty("topic") String topic, @JsonProperty("partition") int partition,
      @JsonProperty("offset") long offset, @JsonProperty("metadata") String metadata
  ) {
    this.topic = topic;
    this.partition = partition;
    this.offset = offset;
    this.metadata = metadata;
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
  public long getOffset() {
    return offset;
  }

  @JsonProperty
  public void setOffset(long offset) {
    this.offset = offset;
  }

  @JsonProperty
  public String getMetadata() {
    return metadata;
  }

  @JsonProperty
  public void setMetadata(String metadata) {
    this.metadata = metadata;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    TopicPartitionOffsetMetadata that = (TopicPartitionOffsetMetadata) o;

    if (offset != that.offset) {
      return false;
    }
    if (!Objects.equals(metadata, that.metadata)) {
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
    result = 31 * result + (int) (offset ^ (offset >>> 32));
    return result;
  }
}
