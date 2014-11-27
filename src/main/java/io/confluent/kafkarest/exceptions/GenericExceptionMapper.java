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
import javax.ws.rs.core.Response;

/**
 * Catch-all exception mapper to handle any uncaught errors that aren't already mapped.
 */
public class GenericExceptionMapper extends DebuggableExceptionMapper<Throwable> {
    public GenericExceptionMapper(Config config) {
        super(config);
    }

    @Override
    public Response toResponse(Throwable exc) {
        // There's no more specific information about the exception that can be passed back to the user, so we can only
        // use the generic message. Debug mode will append the exception info.
        return createResponse(exc, Response.Status.INTERNAL_SERVER_ERROR, Response.Status.INTERNAL_SERVER_ERROR.getReasonPhrase());
    }
}
