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
import static java.util.Objects.requireNonNull;

import com.fasterxml.jackson.annotation.JsonValue;
import com.google.auto.value.AutoValue;
import io.confluent.kafkarest.exceptions.v3.ErrorResponse;
import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.core.Response;
import org.glassfish.jersey.server.ChunkedOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper class to allow transforming a stream of requests into a stream of responses online.
 *
 * <p>Usage:
 *
 * <pre>{@code
 * StreamingResponse.from(myStreamOfRequests) // e.g. a MappingIterator request body
 *     .compose(request -> computeResponse(request))
 *     .resume(asyncResponse);
 * }</pre>
 */
// CHECKSTYLE:OFF:ClassDataAbstractionCoupling
public abstract class StreamingResponse<T> {

  private static final Logger log = LoggerFactory.getLogger(StreamingResponse.class);

  private final ChunkedOutputFactory chunkedOutputFactory;
  private final Duration maxDuration;
  private final Duration gracePeriod;
  private final Instant streamStartTime;
  private final Clock clock;

  volatile boolean closingStarted = false;

  StreamingResponse(
      ChunkedOutputFactory chunkedOutputFactory,
      Duration maxDuration,
      Duration gracePeriod,
      Clock clock) {
    this.clock = clock;
    this.streamStartTime = clock.instant();
    this.chunkedOutputFactory = requireNonNull(chunkedOutputFactory);
    this.maxDuration = maxDuration;
    this.gracePeriod = gracePeriod;
  }

  public static <U> StreamingResponse<U> compose(
      AsyncResponse asyncResponse,
      ChunkedOutputFactory chunkedOutputFactory,
      Duration maxDuration,
      Duration gracePeriod) {
    return new ComposingStreamingResponse<>(
        asyncResponse, chunkedOutputFactory, maxDuration, gracePeriod);
  }

  public static ErrorResponse toErrorResponse(Throwable t) {
    return EXCEPTION_MAPPER.toErrorResponse(t);
  }

  abstract boolean hasNext();

  public abstract void close();

  abstract CompletableFuture<T> next();

  public abstract boolean write(ResultOrError result);

  public abstract void writeError(Throwable error);

  private static final class ComposingStreamingResponse<O> extends StreamingResponse<O> {
    private final ChunkedOutput<ResultOrError> sink;

    private ComposingStreamingResponse(
        AsyncResponse asyncResponse,
        ChunkedOutputFactory chunkedOutputFactory,
        Duration maxDuration,
        Duration gracePeriod) {
      super(chunkedOutputFactory, maxDuration, gracePeriod, Clock.systemUTC());
      this.sink = chunkedOutputFactory.getChunkedOutput();
      asyncResponse.resume(Response.ok(this.sink).build());
    }

    @Override
    public boolean hasNext() {
      throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<O> next() {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean write(ResultOrError r) {
      try {
        sink.write(r);
        return true;
      } catch (IOException e) {
        log.error("Error when writing streaming result to response channel", e);
        return false;
      }
    }

    @Override
    public void writeError(Throwable error) {
      try {
        ResultOrError r = ResultOrError.error(EXCEPTION_MAPPER.toErrorResponse(error));
        sink.write(r);
      } catch (IOException e) {
        log.error("Error when writing error result to response channel", e);
      }
    }

    @Override
    public void close() {
      try {
        sink.close();
      } catch (IOException e) {
        log.error("Error closing output stream", e);
      }
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
// CHECKSTYLE:ON:ClassDataAbstractionCoupling
