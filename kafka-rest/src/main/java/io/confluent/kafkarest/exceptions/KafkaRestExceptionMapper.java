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
import org.apache.kafka.common.errors.SerializationException;

import javax.ws.rs.core.Response;

public final class KafkaRestExceptionMapper extends KafkaExceptionMapper {

  public KafkaRestExceptionMapper(final RestConfig restConfig) {
    super(restConfig);
  }

  @Override
  public Response toResponse(Throwable exception) {
    if (exception instanceof SerializationException) {
      // CPKAFKA-3412: FIXME We should return more specific error codes (unavailable,
      // registration failed, authorization etc).
      return getResponse(exception, Response.Status.REQUEST_TIMEOUT, 40801);
    } else {
      return super.toResponse(exception);
    }
  }

  private Response getResponse(Throwable exception, Response.Status status, int errorCode) {
    ErrorMessage errorMessage = new ErrorMessage(errorCode, exception.getMessage());
    return Response.status(status).entity(errorMessage).build();
  }
}

