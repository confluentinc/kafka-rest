/*
 * Copyright 2023 Confluent Inc.
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

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.jaxrs.base.JsonMappingExceptionMapper;
import com.fasterxml.jackson.jaxrs.base.JsonParseExceptionMapper;
import com.google.common.collect.ImmutableList;
import io.confluent.kafkarest.exceptions.RestConstraintViolationExceptionMapper;
import io.confluent.kafkarest.exceptions.StatusCodeException;
import io.confluent.kafkarest.exceptions.v3.ErrorResponse;
import io.confluent.kafkarest.exceptions.v3.V3ExceptionMapper;
import io.confluent.kafkarest.response.StreamingResponse.ResultOrError;
import io.confluent.rest.entities.ErrorMessage;
import io.confluent.rest.exceptions.KafkaExceptionMapper;
import io.confluent.rest.exceptions.RestConstraintViolationException;
import io.confluent.rest.exceptions.WebApplicationExceptionMapper;
import java.util.List;
import java.util.function.Function;
import javax.annotation.Nullable;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.ext.ExceptionMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class CompositeErrorMapper {
  private static final Logger log = LoggerFactory.getLogger(CompositeErrorMapper.class);

  public static final CompositeErrorMapper EXCEPTION_MAPPER =
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
              RestConstraintViolationException.class,
              new RestConstraintViolationExceptionMapper(),
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

  private final List<ErrorMapper<?>> mappers;
  private final ErrorMapper<Throwable> defaultMapper;

  private CompositeErrorMapper(List<ErrorMapper<?>> mappers, ErrorMapper<Throwable> defaultMapper) {
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

  public static <T> ResultOrError handleNext(T result, @Nullable Throwable error) {
    if (error == null) {
      return ResultOrError.result(result);
    } else {
      log.debug("Error processing streaming operation.", error);
      return ResultOrError.error(EXCEPTION_MAPPER.toErrorResponse(error.getCause()));
    }
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
      String originalMessage = message.apply(response);
      // The example of exception message is `{"error_code":400,"message":"Bad Request: Error
      // processing message: Unexpected character ('o' (code 111)): was expecting comma to separate
      // Object entries\n at [Source:
      // (org.glassfish.jersey.message.internal.ReaderInterceptorExecutor$UnCloseableInputStream);
      // line: 1, column: 4]"}`
      // We don't want to show users the source information, so we need to remove everything after
      // the newline.
      String messageWithoutSource =
          originalMessage == null ? "" : originalMessage.split("\\n")[0].trim();
      return ErrorResponse.create(errorCode.apply(response), messageWithoutSource);
    }
  }
}
