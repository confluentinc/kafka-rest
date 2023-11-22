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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.auto.value.AutoValue;

/**
 * Response returned when an exception happens in a V2 API.
 */
@AutoValue
public abstract class ErrorResponse {

  ErrorResponse() {
  }

  @JsonProperty("error_code")
  public abstract int getErrorCode();

  @JsonProperty("message")
  public abstract String getMessage();

  public static ErrorResponse create(int errorCode, String message) {
    return new AutoValue_ErrorResponse(errorCode, message);
  }

  @JsonCreator
  static ErrorResponse fromJson(
      @JsonProperty("error_code") int errorCode,
      @JsonProperty("message") String message) {
    return create(errorCode, message);
  }
}
