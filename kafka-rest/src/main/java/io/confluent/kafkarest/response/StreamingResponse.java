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
import io.confluent.kafkarest.common.CompletableFutures;
import io.confluent.kafkarest.exceptions.StatusCodeException;
import io.confluent.kafkarest.exceptions.v3.ErrorResponse;
import io.confluent.kafkarest.exceptions.v3.V3ExceptionMapper;
import io.confluent.rest.entities.ErrorMessage;
import io.confluent.rest.exceptions.KafkaExceptionMapper;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import org.glassfish.jersey.server.ChunkedOutput;

// CHECKSTYLE:OFF:ClassDataAbstractionCoupling
public abstract class StreamingResponse<T> {

  private static final JsonMappingExceptionMapper JSON_MAPPING_EXCEPTION_MAPPER =
      new JsonMappingExceptionMapper();
  private static final JsonParseExceptionMapper JSON_PARSE_EXCEPTION_MAPPER =
      new JsonParseExceptionMapper();
  private static final V3ExceptionMapper STATUS_EXCEPTION_MAPPER = new V3ExceptionMapper();
  private static final KafkaExceptionMapper KAFKA_EXCEPTION_MAPPER =
      new KafkaExceptionMapper(/* restConfig= */ null);

  StreamingResponse() {
  }

  public static <T> StreamingResponse<T> from(MappingIterator<T> input) {
    return new InputStreamingResponse<>(input);
  }

  public final <O> StreamingResponse<O> compose(
      Function<? super T, ? extends CompletableFuture<O>> transform) {
    return new ComposingStreamingResponse<>(this, transform);
  }

  public final void resume(AsyncResponse asyncResponse) {
    AsyncResponseQueue responseQueue = new AsyncResponseQueue();
    responseQueue.asyncResume(asyncResponse);
    while (hasNext()) {
      responseQueue.push(next().handle(this::handleNext));
    }
    responseQueue.close();
  }

  private ResultOrError handleNext(T result, Throwable error) {
    if (error == null) {
      return ResultOrError.result(result);
    } else if (error.getCause() instanceof JsonMappingException) {
      Response response =
          JSON_MAPPING_EXCEPTION_MAPPER.toResponse((JsonMappingException) error.getCause());
      return ResultOrError.error(
          new ErrorResponse(
              String.valueOf(Status.BAD_REQUEST.getStatusCode()),
              (String) response.getEntity()));
    } else if (error.getCause() instanceof JsonParseException) {
      Response response =
          JSON_PARSE_EXCEPTION_MAPPER.toResponse((JsonParseException) error.getCause());
      return ResultOrError.error(
          new ErrorResponse(
              String.valueOf(Status.BAD_REQUEST.getStatusCode()),
              (String) response.getEntity()));
    } else if (error.getCause() instanceof StatusCodeException) {
      Response response =
          STATUS_EXCEPTION_MAPPER.toResponse((StatusCodeException) error.getCause());
      return ResultOrError.error((ErrorResponse) response.getEntity());
    } else {
      Response response = KAFKA_EXCEPTION_MAPPER.toResponse(error.getCause());
      ErrorMessage errorMessage = (ErrorMessage) response.getEntity();
      return ResultOrError.error(
          new ErrorResponse(
              String.valueOf(errorMessage.getErrorCode()), errorMessage.getMessage()));
    }
  }

  abstract boolean hasNext();

  abstract CompletableFuture<T> next();

  private static final class InputStreamingResponse<T> extends StreamingResponse<T> {
    private final MappingIterator<T> input;

    private InputStreamingResponse(MappingIterator<T> input) {
      this.input = requireNonNull(input);
    }

    @Override
    public boolean hasNext() {
      return input.hasNext();
    }

    @Override
    public CompletableFuture<T> next() {
      try {
        return CompletableFuture.completedFuture(input.nextValue());
      } catch (Throwable e) {
        return CompletableFutures.failedFuture(e);
      }
    }
  }

  private static final class ComposingStreamingResponse<I, O> extends StreamingResponse<O> {
    private final StreamingResponse<I> input;
    private final Function<? super I, ? extends CompletableFuture<O>> transform;

    private ComposingStreamingResponse(
        StreamingResponse<I> input, Function<? super I, ? extends CompletableFuture<O>> transform) {
      this.input = requireNonNull(input);
      this.transform = requireNonNull(transform);
    }

    @Override
    public boolean hasNext() {
      return input.hasNext();
    }

    @Override
    public CompletableFuture<O> next() {
      return input.next().thenCompose(transform);
    }
  }

  private static final class AsyncResponseQueue {
    private static final String CHUNK_SEPARATOR = "\r\n";

    private final ChunkedOutput<ResultOrError> sink;
    private CompletableFuture<Void> tail;

    private AsyncResponseQueue() {
      sink = new ChunkedOutput<>(ResultOrError.class, CHUNK_SEPARATOR);
      tail = CompletableFuture.completedFuture(null);
    }

    private void asyncResume(AsyncResponse asyncResponse) {
      asyncResponse.resume(Response.ok(sink).build());
    }

    private void push(CompletableFuture<ResultOrError> result) {
      tail =
          CompletableFuture.allOf(tail, result)
              .thenApply(
                  unused -> {
                    try {
                      sink.write(result.join());
                    } catch (IOException e) {
                      // There's not much else we can do if we fail write the response back.
                      e.printStackTrace();
                    }
                    return null;
                  });
    }

    private void close() {
      tail.whenComplete(
          (unused, throwable) -> {
            try {
              sink.close();
            } catch (IOException e) {
              // There's not much else we can do if we fail to close the response channel.
              e.printStackTrace();
            }
          });
    }
  }

  private abstract static class ResultOrError {

    private static <T> ResultHolder<T> result(T result) {
      return new AutoValue_StreamingResponse_ResultHolder<>(result);
    }

    private static ErrorHolder error(ErrorResponse error) {
      return new AutoValue_StreamingResponse_ErrorHolder(error);
    }
  }

  @AutoValue
  abstract static class ResultHolder<T> extends ResultOrError {

    ResultHolder() {
    }

    @JsonValue
    abstract T getResult();
  }

  @AutoValue
  abstract static class ErrorHolder extends ResultOrError {

    ErrorHolder() {
    }

    @JsonValue
    abstract ErrorResponse getError();
  }
}
// CHECKSTYLE:ON:ClassDataAbstractionCoupling