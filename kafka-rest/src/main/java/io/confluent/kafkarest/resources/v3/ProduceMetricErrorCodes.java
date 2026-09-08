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
   * Resolve an HTTP status code for the throwable in the same way StreamingResponse does for the
   * response, so the produce-api metric's http_status_code tag stays consistent with what the
   * client actually sees. Unwraps a single layer of CompletionException to match how
   * StreamingResponse itself handles errors handed off from the CompletableFuture chain.
   */
  static int httpStatusCodeFromError(Throwable error) {
    Throwable cause = error;
    if (error instanceof CompletionException && error.getCause() != null) {
      cause = error.getCause();
    }
    return StreamingResponse.toErrorResponse(cause).getErrorCode();
  }
}
