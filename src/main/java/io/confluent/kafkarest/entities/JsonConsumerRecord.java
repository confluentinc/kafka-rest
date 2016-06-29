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

public class JsonConsumerRecord extends AbstractConsumerRecord<Object, Object> {

  public JsonConsumerRecord(@JsonProperty("key") Object key,
                            @JsonProperty("value") Object value,
                            @JsonProperty("topic") String topic,
                            @JsonProperty("partition") int partition,
                            @JsonProperty("offset") long offset) {
    super(key, value, topic, partition, offset);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    JsonConsumerRecord that = (JsonConsumerRecord) o;
    if (topic != null ? !topic.equals(that.topic) : that.topic != null) return false;
    if (partition != that.partition) return false;
    if (offset != that.offset) return false;
    if (key != null ? !key.equals(that.key) : that.key != null) return false;
    return !(value != null ? !value.equals(that.value) : that.value != null);

  }

  @Override
  public int hashCode() {
    int result = key != null ? key.hashCode() : 0;
    result = 31 * result + (value != null ? value.hashCode() : 0);
    result = 31 * result + (topic != null ? topic.hashCode() : 0);
    result = 31 * result + partition;
    result = 31 * result + (int) (offset ^ (offset >>> 32));
    return result;
  }
}
