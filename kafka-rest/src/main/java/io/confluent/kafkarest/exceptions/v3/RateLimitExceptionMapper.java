/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.kafkarest.exceptions.v3;

import io.confluent.kafkarest.exceptions.RateLimitGracePeriodExceededException;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;

public class RateLimitExceptionMapper
    implements ExceptionMapper<RateLimitGracePeriodExceededException> {

  @Override
  public Response toResponse(RateLimitGracePeriodExceededException exception) {
    return Response.status(444)
        .entity(ErrorResponse.create(444, "Rate limited exceeded for longer than grace period"))
        .build();
  }
}
