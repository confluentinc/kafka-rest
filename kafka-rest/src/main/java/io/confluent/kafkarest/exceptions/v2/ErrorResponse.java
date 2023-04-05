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

package io.confluent.kafkarest.exceptions.v2;

import static java.util.Objects.requireNonNull;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;
import java.util.StringJoiner;

/**
 * Response returned when an exception happens in a V2 API.
 */
public final class ErrorResponse {

  private final String errorCode;

  private final String message;

  public ErrorResponse(String errorCode, String message) {
    this.errorCode = requireNonNull(errorCode);
    this.message = requireNonNull(message);
  }

  @JsonProperty("error_code")
  public String getErrorCode() {
    return errorCode;
  }

  @JsonProperty("message")
  public String getMessage() {
    return message;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ErrorResponse that = (ErrorResponse) o;
    return errorCode.equals(that.errorCode) && message.equals(that.message);
  }

  @Override
  public int hashCode() {
    return Objects.hash(errorCode, message);
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", ErrorResponse.class.getSimpleName() + "[", "]")
        .add("errorCode='" + errorCode + "'")
        .add("message='" + message + "'")
        .toString();
  }
}
