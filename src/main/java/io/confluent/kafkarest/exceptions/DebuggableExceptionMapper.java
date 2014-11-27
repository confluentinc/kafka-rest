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
import io.confluent.kafkarest.entities.ErrorMessage;

import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;

/**
 * Abstract exception mapper that checks the debug flag and generates an error message including the stack trace if it
 * is enabled.
 */
@Provider
public abstract class DebuggableExceptionMapper<E extends Throwable> implements ExceptionMapper<E> {
    Config config;

    @Context
    HttpHeaders headers;

    public DebuggableExceptionMapper(Config config) {
        this.config = config;
    }

    /**
     * Create a Response object using the given exception, status, and message. When debugging is enabled, the message
     * will be replaced with the exception class, exception message, and stacktrace.
     * @param exc Throwable that triggered this ExceptionMapper
     * @param status HTTP response status
     * @param msg
     * @return
     */
    public Response createResponse(Throwable exc, Response.Status status, String msg) {
        String readableMessage = msg;
        if (config != null && config.debug) {
            readableMessage += " " + exc.getClass().getName() + ": " + exc.getMessage();
            try {
                ByteArrayOutputStream os = new ByteArrayOutputStream();
                PrintStream stream = new PrintStream(os);
                exc.printStackTrace(stream);
                stream.close();
                os.close();
                readableMessage += "\n" + os.toString();
            } catch (IOException e) {
                // Ignore
            }
        }
        final ErrorMessage message = new ErrorMessage(status.getStatusCode(), readableMessage);

        return Response.status(status)
                .entity(message)
                .type(MediaType.APPLICATION_JSON)
                .build();
    }

}
