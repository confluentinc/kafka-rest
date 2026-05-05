/*
 * Copyright 2026 Confluent Inc.
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

import jakarta.ws.rs.BadRequestException;
import java.util.Locale;

/**
 * Filter values for the {@code topic_type} query parameter on the list-topics endpoint. Used to
 * select between regular topics and view topics in the response.
 */
public enum TopicType {
  /** Return all topics (regular and view topics). Default behavior when the param is absent. */
  ALL,

  /** Return only non-view topics. */
  STANDARD,

  /** Return only view topics. */
  VIEW;

  /**
   * Parses a {@code topic_type} query-parameter string into the corresponding enum value. Null or
   * empty input maps to {@link #ALL}. Invalid values raise a 400.
   */
  public static TopicType fromString(String value) {
    if (value == null || value.isEmpty()) {
      return ALL;
    }
    try {
      return TopicType.valueOf(value.toUpperCase(Locale.ROOT));
    } catch (IllegalArgumentException e) {
      throw new BadRequestException(
          "Invalid topic_type '" + value + "'. Expected one of: all, standard, view.");
    }
  }
}
