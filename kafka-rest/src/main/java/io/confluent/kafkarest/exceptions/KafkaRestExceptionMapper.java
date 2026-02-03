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

package io.confluent.kafkarest.exceptions;

import io.confluent.rest.RestConfig;
import io.confluent.rest.entities.ErrorMessage;
import io.confluent.rest.exceptions.KafkaExceptionMapper;
import jakarta.ws.rs.core.Response;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.common.errors.SerializationException;

public final class KafkaRestExceptionMapper extends KafkaExceptionMapper {

  private static final int KAFKA_BAD_REQUEST_ERROR_CODE = 40002;

  public KafkaRestExceptionMapper(final RestConfig restConfig) {
    super(restConfig);
  }

  @Override
  public Response toResponse(Throwable exception) {
    Throwable cause = unwrapException(exception);

    if (cause instanceof InvalidConfigurationWithSchemaException) {
      return handleSchemaValidationError((InvalidConfigurationWithSchemaException) cause);
    } else if (exception instanceof SerializationException) {
      // CPKAFKA-3412: FIXME We should return more specific error codes (unavailable,
      // registration failed, authorization etc).
      return getResponse(exception, Response.Status.REQUEST_TIMEOUT, 40801);
    } else {
      return super.toResponse(exception);
    }
  }

  private Throwable unwrapException(Throwable exception) {
    if (exception instanceof ExecutionException || exception instanceof CompletionException) {
      return exception.getCause() != null ? exception.getCause() : exception;
    }
    return exception;
  }

  private Response handleSchemaValidationError(InvalidConfigurationWithSchemaException exception) {
    int schemaErrorCode = exception.getSchemaErrorCode();
    // schemaErrorCode 0 means no schema error per Odyssey contracts; convert to null to omit field
    SchemaErrorMessage errorMessage =
        new SchemaErrorMessage(
            KAFKA_BAD_REQUEST_ERROR_CODE,
            exception.getMessage(),
            schemaErrorCode != 0 ? schemaErrorCode : null);
    return Response.status(Response.Status.BAD_REQUEST).entity(errorMessage).build();
  }

  private Response getResponse(Throwable exception, Response.Status status, int errorCode) {
    ErrorMessage errorMessage = new ErrorMessage(errorCode, exception.getMessage());
    return Response.status(status).entity(errorMessage).build();
  }
}
