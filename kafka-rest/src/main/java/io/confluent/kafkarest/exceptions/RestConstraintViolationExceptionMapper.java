/*
 * Copyright 2022 Confluent Inc.
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

import io.confluent.kafkarest.exceptions.v3.ErrorResponse;
import io.confluent.rest.exceptions.RestConstraintViolationException;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;

public final class RestConstraintViolationExceptionMapper
    implements ExceptionMapper<RestConstraintViolationException> {

  @Override
  public Response toResponse(RestConstraintViolationException exception) {
    return Response.status(exception.getStatus())
        .entity(
            ErrorResponse.create(
                exception.getStatus(),
                String.format("Error: %s : %s", exception.getErrorCode(), exception.getMessage())))
        .build();
  }
}
