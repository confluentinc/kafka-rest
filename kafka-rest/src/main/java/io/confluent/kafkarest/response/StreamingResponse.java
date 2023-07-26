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

import static io.confluent.kafkarest.response.CompositeErrorMapper.EXCEPTION_MAPPER;

import com.fasterxml.jackson.annotation.JsonValue;
import com.google.auto.value.AutoValue;
import io.confluent.kafkarest.exceptions.StatusCodeException;
import io.confluent.kafkarest.exceptions.v3.ErrorResponse;
import io.confluent.kafkarest.requests.JsonStreamIterable;
import java.time.Duration;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import org.glassfish.jersey.server.ChunkedOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class StreamingResponse<I> {

  private static final Logger log = LoggerFactory.getLogger(StreamingResponse.class);

  private final Duration maxDuration;
  private final Duration gracePeriod;

  private final ChunkedOutput<ResultOrError> sink;
  private final JsonStreamIterable<I> source;

  private StreamingResponse(
      JsonStreamIterable<I> inputStream,
      AsyncResponse asyncResponse,
      ChunkedOutputFactory chunkedOutputFactory,
      Duration maxDuration,
      Duration gracePeriod,
      ScheduledExecutorService executorService) {
    this.maxDuration = maxDuration;
    this.gracePeriod = gracePeriod;
    this.source = inputStream;
    this.sink = chunkedOutputFactory.getChunkedOutput();

    asyncResponse.resume(Response.ok(this.sink).build());
    scheduleConnectionCloseOnOverMaxDuration(executorService);
  }

  private void scheduleConnectionCloseOnOverMaxDuration(ScheduledExecutorService executorService) {
    if (this.gracePeriod.isZero() || this.gracePeriod.isNegative()) {
      executorService.schedule(
          () -> {
            writeResult(
                ResultOrError.error(
                    EXCEPTION_MAPPER.toErrorResponse(
                        new StatusCodeException(
                            Status.REQUEST_TIMEOUT,
                            "Streaming connection open for longer than allowed",
                            "Connection will be closed."))));
            this.close();
          },
          this.maxDuration.toMillis(),
          TimeUnit.MILLISECONDS);
    } else {
      executorService.schedule(
          () ->
              writeResult(
                  ResultOrError.error(
                      EXCEPTION_MAPPER.toErrorResponse(
                          new StatusCodeException(
                              Status.REQUEST_TIMEOUT,
                              "Streaming connection open for longer than allowed",
                              "Connection will be closed.")))),
          this.maxDuration.toMillis(),
          TimeUnit.MILLISECONDS);
      executorService.schedule(
              this::close,
          this.maxDuration.toMillis() + gracePeriod.toMillis(),
          TimeUnit.MILLISECONDS);
    }
  }

  public static <I> StreamingResponse<I> compose(
      JsonStreamIterable<I> jsonStreamWrapper,
      AsyncResponse asyncResponse,
      ChunkedOutputFactory chunkedOutputFactory,
      Duration maxDuration,
      Duration gracePeriod,
      ScheduledExecutorService executorService) {
    return new StreamingResponse<>(
        jsonStreamWrapper,
        asyncResponse,
        chunkedOutputFactory,
        maxDuration,
        gracePeriod,
        executorService);
  }

  public static ErrorResponse toErrorResponse(Throwable t) {
    return EXCEPTION_MAPPER.toErrorResponse(t);
  }

  public void writeResult(ResultOrError r) {
    try {
      sink.write(r);
    } catch (Throwable e) {
      log.debug("Error when writing streaming result to response channel", e);
    }
  }

  public void writeError(Throwable error) {
    try {
      sink.write(ResultOrError.error(EXCEPTION_MAPPER.toErrorResponse(error)));
    } catch (Throwable e) {
      log.debug("Error when writing error result to response channel", e);
    }
  }

  public void close() {
    try {
      source.close();
    } catch (Throwable e) {
      log.debug("Error closing input", e);
    }

    try {
      sink.close();
    } catch (Throwable e) {
      log.debug("Error closing output", e);
    }
  }

  public abstract static class ResultOrError {

    public static <T> ResultHolder<T> result(T result) {
      return new AutoValue_StreamingResponse_ResultHolder<>(result);
    }

    public static ErrorHolder error(ErrorResponse error) {
      return new AutoValue_StreamingResponse_ErrorHolder(error);
    }
  }

  @AutoValue
  abstract static class ResultHolder<T> extends ResultOrError {

    ResultHolder() {}

    @JsonValue
    abstract T getResult();
  }

  @AutoValue
  abstract static class ErrorHolder extends ResultOrError {

    ErrorHolder() {}

    @JsonValue
    abstract ErrorResponse getError();
  }
}
