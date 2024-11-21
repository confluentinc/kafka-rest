/*
 * Copyright 2021 Confluent Inc.
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

package io.confluent.kafkarest.ratelimit;

import io.confluent.kafkarest.exceptions.StatusCodeException;
import javax.ws.rs.core.Response;

/**
 * An exception thrown when the request rate-limit has been exceeded.
 *
 * @see RequestRateLimiter#rateLimit(int)
 */
public final class RateLimitExceededException extends StatusCodeException {

  /*
   *  These error-codes signify which rate-limit got exceeded, and then
   *  logged with request in CustomLog.
   */
  public static class ErrorCodes {

    // Only apply to Produce APIs.
    public static final int PRODUCE_MAX_REQUESTS_GLOBAL_LIMIT_EXCEEDED = 429001;
    public static final int PRODUCE_MAX_REQUESTS_PER_TENANT_LIMIT_EXCEEDED = 429002;
    public static final int PRODUCE_MAX_BYTES_GLOBAL_LIMIT_EXCEEDED = 429003;
    public static final int PRODUCE_MAX_BYTES_PER_TENANT_LIMIT_EXCEEDED = 429004;

    // Only apply to Admin APIs for now.
    public static final int PERMITS_MAX_GLOBAL_LIMIT_EXCEEDED = 429005;
    public static final int PERMITS_MAX_PER_CLUSTER_LIMIT_EXCEEDED = 429006;

    // Apply to all APIs, via the Dos-filters at Jetty layer.
    public static final int DOS_FILTER_MAX_REQUEST_LIMIT_EXCEEDED = 429007;
    public static final int DOS_FILTER_MAX_REQUEST_PER_CONNECTION_LIMIT_EXCEEDED = 429008;
  }

  public RateLimitExceededException() {
    super(
        Response.Status.TOO_MANY_REQUESTS,
        "Request rate limit exceeded",
        "The rate limit of requests per second has been exceeded.");
  }

  @Override
  public synchronized Throwable fillInStackTrace() {
    // Skipping stack-trace filling, as these exceptions are being used as a specific signal with
    // well-known origin (our rate-limiting infrastructure).
    return this;
  }
}
