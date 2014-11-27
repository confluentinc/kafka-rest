/**
 * Copyright 2014 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.confluent.kafkarest.exceptions;

import io.confluent.kafkarest.Config;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;

public class WebApplicationExceptionMapper extends DebuggableExceptionMapper<WebApplicationException> {
    public WebApplicationExceptionMapper(Config config) {
        super(config);
    }

    @Override
    public Response toResponse(WebApplicationException exc) {
        // WebApplicationException unfortunately doesn't expose the status, or even status code, directly.
        Response.Status status = Response.Status.fromStatusCode(exc.getResponse().getStatus());
        // The human-readable message for these can use the exception message directly. Since WebApplicationExceptions
        // are expected to be passed back to users, it will either contain a situation-specific message or the HTTP status
        // message
        return createResponse(exc, status, exc.getMessage());
    }
}
