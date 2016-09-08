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
import com.fasterxml.jackson.databind.JsonNode;

public class AvroConsumerRecord extends AbstractConsumerRecord<JsonNode, JsonNode> {

  public AvroConsumerRecord(
      @JsonProperty("key") JsonNode key, @JsonProperty("value") JsonNode value,
      @JsonProperty("topic") String topic, @JsonProperty("partition") int partition,
      @JsonProperty("offset") long offset
  ) {
    super(key, value, topic, partition, offset);
  }
}
