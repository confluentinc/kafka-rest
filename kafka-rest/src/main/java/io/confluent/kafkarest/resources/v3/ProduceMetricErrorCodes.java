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

package io.confluent.kafkarest.resources.v3;

import io.confluent.kafkarest.response.StreamingResponse;
import java.util.concurrent.CompletionException;

final class ProduceMetricErrorCodes {

  private ProduceMetricErrorCodes() {}

  /**
   * Resolves the HTTP status StreamingResponse would return for the error. Its error codes are
   * either three-digit statuses or five-digit REST codes (e.g. 40301) whose first three digits are
   * the status.
   */
  static int httpStatusCodeFromError(Throwable error) {
    Throwable cause = error;
    if (error instanceof CompletionException && error.getCause() != null) {
      cause = error.getCause();
    }
    int errorCode = StreamingResponse.toErrorResponse(cause).getErrorCode();
    return errorCode >= 1000 ? errorCode / 100 : errorCode;
  }
}
