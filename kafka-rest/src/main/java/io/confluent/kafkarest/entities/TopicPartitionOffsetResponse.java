/*
 * Copyright 2019 Confluent Inc.
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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonProperty.Access;

/**
 * Response for GET /topics/(topic)/partitions/(partition)/offsets requests.
 *
 * @see io.confluent.kafkarest.resources.v2.PartitionsResource#getOffsets(String, int)
 */
public final class TopicPartitionOffsetResponse {

  private final long beginningOffset;
  private final long endOffset;

  public TopicPartitionOffsetResponse(long beginningOffset, long endOffset) {
    this.beginningOffset = beginningOffset;
    this.endOffset = endOffset;
  }

  /**
   * The earliest offset in the topic partition.
   */
  @JsonProperty(value = "beginning_offset", access = Access.READ_ONLY)
  public long getBeginningOffset() {
    return beginningOffset;
  }

  /**
   * The latest offset in the topic partition.
   */
  @JsonProperty(value = "end_offset", access = Access.READ_ONLY)
  public long getEndOffset() {
    return endOffset;
  }
}
