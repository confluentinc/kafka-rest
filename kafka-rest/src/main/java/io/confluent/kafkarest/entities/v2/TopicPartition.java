/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.kafkarest.entities.v2;

import com.fasterxml.jackson.annotation.JsonProperty;
import javax.validation.constraints.Min;
import org.hibernate.validator.constraints.NotEmpty;

public class TopicPartition {

  @NotEmpty
  private String topic;
  @Min(0)
  private int partition;
    
  public TopicPartition(
      @JsonProperty("topic") String topic, @JsonProperty("partition") int partition
  ) {
    this.topic = topic;
    this.partition = partition;
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

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    TopicPartition that = (TopicPartition) o;

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
    return result;
  }
}
