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

package io.confluent.kafkarest.response;

import static java.util.Objects.requireNonNull;

import com.google.common.util.concurrent.TimeLimiter;
import io.confluent.kafkarest.exceptions.v3.ErrorResponse;
import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;

public final class StreamingResponseIdleTimeLimiter {
  private final TimeLimiter delegate;
  private final Duration maxIdleTime;

  public StreamingResponseIdleTimeLimiter(TimeLimiter delegate, Duration maxIdleTime) {
    this.delegate = requireNonNull(delegate);
    this.maxIdleTime = requireNonNull(maxIdleTime);
  }

  <T> T callWithTimeout(Callable<T> callable)
      throws ExecutionException, InterruptedException, MaxIdleTimeExceededException {
    try {
      return delegate.callWithTimeout(callable, maxIdleTime);
    } catch (TimeoutException e) {
      throw new MaxIdleTimeExceededException(e);
    }
  }

  static final class MaxIdleTimeExceededException extends RuntimeException {

    MaxIdleTimeExceededException(Throwable cause) {
      super(cause);
    }
  }

  static final class MaxIdleTimeExceededExceptionMapper
      implements ExceptionMapper<MaxIdleTimeExceededException> {

    @Override
    public Response toResponse(MaxIdleTimeExceededException exception) {
      return Response.status(Response.Status.REQUEST_TIMEOUT)
          .entity(
              ErrorResponse.create(
                  Response.Status.REQUEST_TIMEOUT.getStatusCode(),
                  "Timeout while reading from inputStream"))
          .build();
    }
  }
}
