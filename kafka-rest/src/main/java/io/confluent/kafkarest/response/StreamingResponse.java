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

import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.jaxrs.base.JsonMappingExceptionMapper;
import com.fasterxml.jackson.jaxrs.base.JsonParseExceptionMapper;
import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableList;
import io.confluent.kafkarest.common.CompletableFutures;
import io.confluent.kafkarest.exceptions.StatusCodeException;
import io.confluent.kafkarest.exceptions.v3.ErrorResponse;
import io.confluent.kafkarest.exceptions.v3.V3ExceptionMapper;
import io.confluent.kafkarest.tracing.Tracer;
import io.confluent.rest.entities.ErrorMessage;
import io.confluent.rest.exceptions.KafkaExceptionMapper;
import io.confluent.rest.exceptions.WebApplicationExceptionMapper;
import java.io.IOException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import javax.annotation.Nullable;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.ext.ExceptionMapper;
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

  private static final CompositeErrorMapper EXCEPTION_MAPPER =
      new CompositeErrorMapper.Builder()
          .putMapper(
              JsonMappingException.class,
              new JsonMappingExceptionMapper(),
              response -> Status.BAD_REQUEST.getStatusCode(),
              response -> (String) response.getEntity())
          .putMapper(
              JsonParseException.class,
              new JsonParseExceptionMapper(),
              response -> Status.BAD_REQUEST.getStatusCode(),
              response -> (String) response.getEntity())
          .putMapper(
              StatusCodeException.class,
              new V3ExceptionMapper(),
              response -> ((ErrorResponse) response.getEntity()).getErrorCode(),
              response -> ((ErrorResponse) response.getEntity()).getMessage())
          .putMapper(
              WebApplicationException.class,
              new WebApplicationExceptionMapper(/* restConfig= */ null),
              response -> ((ErrorMessage) response.getEntity()).getErrorCode(),
              response -> ((ErrorMessage) response.getEntity()).getMessage())
          .setDefaultMapper(
              new KafkaExceptionMapper(/* restConfig= */ null),
              response -> ((ErrorMessage) response.getEntity()).getErrorCode(),
              response -> ((ErrorMessage) response.getEntity()).getMessage())
          .build();

  private final ChunkedOutputFactory chunkedOutputFactory;

  StreamingResponse(ChunkedOutputFactory chunkedOutputFactory) {
    this.chunkedOutputFactory = requireNonNull(chunkedOutputFactory);
  }

  public static <T> StreamingResponse<T> from(
      MappingIterator<T> mappingIteratorInput, ChunkedOutputFactory chunkedOutputFactory) {
    return new InputStreamingResponse<>(mappingIteratorInput, chunkedOutputFactory);
  }

  public final <O> StreamingResponse<O> compose(
      Function<? super T, ? extends CompletableFuture<O>> transform) {
    return new ComposingStreamingResponse<>(this, transform, chunkedOutputFactory);
  }

  // TODO ddimitrov Make traceId an instance field in a way minimally intrusive to tests.
  /**
   * Stream requests in and start transforming them into responses.
   *
   * <p>This method will block until all requests are read in. The responses are computed and
   * written to {@code asyncResponse} asynchronously.
   */
  public final void resume(AsyncResponse asyncResponse, UUID traceId) {
    log.debug("Resuming StreamingResponse");
    AsyncResponseQueue responseQueue = new AsyncResponseQueue(chunkedOutputFactory);
    responseQueue.asyncResume(asyncResponse);
    while (hasNext() && !responseQueue.isClosed()) {
      responseQueue.push(next().handle((result, error) -> handleNext(result, error, traceId)));
    }
    close();
    responseQueue.close(traceId);
  }

  private ResultOrError handleNext(T result, @Nullable Throwable error, UUID traceId) {
    if (error == null) {
      Tracer.trace(traceId, "Successfully produced a record");
      return ResultOrError.result(result);
    } else {
      Tracer.trace(traceId, "Error %s while producing a record", error);
      log.debug("Error processing streaming operation.", error);
      return ResultOrError.error(EXCEPTION_MAPPER.toErrorResponse(error.getCause()));
    }
  }

  abstract boolean hasNext();

  abstract void close();

  abstract CompletableFuture<T> next();

  private static class InputStreamingResponse<T> extends StreamingResponse<T> {

    private final MappingIterator<T> mappingIteratorInput;

    private InputStreamingResponse(
        MappingIterator<T> mappingIteratorInput, ChunkedOutputFactory chunkedOutputFactory) {
      super(chunkedOutputFactory);
      this.mappingIteratorInput = requireNonNull(mappingIteratorInput);
    }

    public void close() {
      try {
        mappingIteratorInput.close();
      } catch (IOException e) {
        log.error("Error when closing the request stream.", e);
      }
    }

    @Override
    public boolean hasNext() {
      return mappingIteratorInput.hasNext();
    }

    @Override
    public CompletableFuture<T> next() {
      try {
        return CompletableFuture.completedFuture(mappingIteratorInput.nextValue());
      } catch (Throwable e) {
        return CompletableFutures.failedFuture(e);
      }
    }
  }

  private static final class ComposingStreamingResponse<I, O> extends StreamingResponse<O> {

    private final StreamingResponse<I> streamingResponseInput;
    private final Function<? super I, ? extends CompletableFuture<O>> transform;

    private ComposingStreamingResponse(
        StreamingResponse<I> streamingResponseInput,
        Function<? super I, ? extends CompletableFuture<O>> transform,
        ChunkedOutputFactory chunkedOutputFactory) {
      super(chunkedOutputFactory);
      this.streamingResponseInput = requireNonNull(streamingResponseInput);
      this.transform = requireNonNull(transform);
    }

    @Override
    public boolean hasNext() {
      return streamingResponseInput.hasNext();
    }

    @Override
    public CompletableFuture<O> next() {
      return streamingResponseInput.next().thenCompose(transform);
    }

    public void close() {
      streamingResponseInput.close();
    }
  }

  private static final class AsyncResponseQueue {

    private final ChunkedOutput<ResultOrError> sink;

    // tail is the end of a linked list of completable futures. The futures are tied together by
    // allOf completion handles. For example, let's say we push 3 futures into the queue:
    //
    // 1. tail = (null) // empty queue
    // 2. tail = then(allOf((null), f_1), write(f_1)) // -> f_1
    // 3. tail = then(allOf(then(allOf((null), f_1), write(f_1)), f_2), write(f_2)) // -> f_1 -> f_2
    //
    // The chaining of the futures guarantees that the write(f) callbacks are called in the order
    // that the futures got pushed into the queue. Also, due to the way allOf is implemented, it
    // makes sure that, as soon as f_0..i is completed, the whole
    // then(allOf((f_i-1), f_i), write(f_i)) monad is made available for garbage collection.
    private CompletableFuture<Void> tail;

    private AsyncResponseQueue(ChunkedOutputFactory chunkedOutputFactory) {
      sink = chunkedOutputFactory.getChunkedOutput();
      tail = CompletableFuture.completedFuture(null);
    }

    private void asyncResume(AsyncResponse asyncResponse) {
      asyncResponse.resume(Response.ok(sink).build());
    }

    private volatile boolean sinkClosed = false;

    private boolean isClosed() {
      return sinkClosed;
    }

    private void push(CompletableFuture<ResultOrError> result) {
      log.debug("Pushing to response queue");
      tail =
          CompletableFuture.allOf(tail, result)
              .thenApply(
                  unused -> {
                    try {
                      if (sinkClosed || sink.isClosed()) {
                        sinkClosed = true;
                        return null;
                      }
                      ResultOrError res = result.join();
                      log.debug("Writing to sink");
                      sink.write(res);
                      if (res instanceof ErrorHolder
                          && ((ErrorHolder) res).getError().getErrorCode() == 429) {
                        log.warn(
                            "Connection closed as a result of rate limiting being exceeded "
                                + "for longer than grace period.");
                        sinkClosed = true;
                        sink.close();
                      }
                    } catch (IOException e) {
                      log.error("Error when writing streaming result to response channel.", e);
                    }
                    return null;
                  });
    }

    private void close(UUID traceId) {
      tail.whenComplete(
          (unused, throwable) -> {
            Tracer.trace(traceId, "Closing the produce batch or stream");
            try {
              sinkClosed = true;
              sink.close();
            } catch (IOException e) {
              log.error("Error when closing response channel.", e);
            }
          });
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

  private static final class ErrorMapper<T extends Throwable> {

    private final Class<T> errorClass;
    private final ExceptionMapper<T> mapper;
    private final Function<Response, Integer> errorCode;
    private final Function<Response, String> message;

    private ErrorMapper(
        Class<T> errorClass,
        ExceptionMapper<T> mapper,
        Function<Response, Integer> errorCode,
        Function<Response, String> message) {
      this.errorClass = requireNonNull(errorClass);
      this.mapper = requireNonNull(mapper);
      this.errorCode = requireNonNull(errorCode);
      this.message = requireNonNull(message);
    }

    private boolean handles(Throwable error) {
      return errorClass.isInstance(error);
    }

    @SuppressWarnings("unchecked")
    private ErrorResponse toErrorResponse(Throwable error) {
      Response response = mapper.toResponse((T) error);
      return ErrorResponse.create(errorCode.apply(response), message.apply(response));
    }
  }

  private static final class CompositeErrorMapper {

    private final List<ErrorMapper<?>> mappers;
    private final ErrorMapper<Throwable> defaultMapper;

    private CompositeErrorMapper(
        List<ErrorMapper<?>> mappers, ErrorMapper<Throwable> defaultMapper) {
      this.mappers = requireNonNull(mappers);
      this.defaultMapper = requireNonNull(defaultMapper);
    }

    public ErrorResponse toErrorResponse(Throwable exception) {
      for (ErrorMapper<?> mapper : mappers) {
        if (mapper.handles(exception)) {
          return mapper.toErrorResponse(exception);
        }
      }
      return defaultMapper.toErrorResponse(exception);
    }

    private static final class Builder {

      private final ImmutableList.Builder<ErrorMapper<?>> mappers = ImmutableList.builder();
      private ErrorMapper<Throwable> defaultMapper;

      private Builder() {}

      private <T extends Throwable> Builder putMapper(
          Class<T> mappedType,
          ExceptionMapper<T> mapper,
          Function<Response, Integer> errorCode,
          Function<Response, String> message) {
        mappers.add(new ErrorMapper<>(mappedType, mapper, errorCode, message));
        return this;
      }

      private Builder setDefaultMapper(
          ExceptionMapper<Throwable> mapper,
          Function<Response, Integer> errorCode,
          Function<Response, String> message) {
        defaultMapper = new ErrorMapper<>(Throwable.class, mapper, errorCode, message);
        return this;
      }

      public CompositeErrorMapper build() {
        return new CompositeErrorMapper(mappers.build(), defaultMapper);
      }
    }
  }
}
// CHECKSTYLE:ON:ClassDataAbstractionCoupling
