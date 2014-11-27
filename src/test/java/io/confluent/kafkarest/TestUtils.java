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
package io.confluent.kafkarest;

import io.confluent.kafkarest.entities.ErrorMessage;

import javax.ws.rs.core.Response;

import static org.junit.Assert.assertEquals;

public class TestUtils {
    /**
     * Asserts that the correct HTTP status code was set for the error and that a generic structured response is returned.
     * This requires a custom message to check against since they should always be provided or explicitly specify the
     * default.
     */
    public static void assertErrorResponse(Response.Status status, Response rawResponse, String msg) {
        assertEquals(status.getStatusCode(), rawResponse.getStatus());

        // Successful deletion's return no content, so we shouldn't try to decode their entities
        if (status.equals(Response.Status.NO_CONTENT)) return;

        ErrorMessage response = rawResponse.readEntity(ErrorMessage.class);
        assertEquals(status.getStatusCode(), response.getErrorCode());
        assertEquals(msg, response.getMessage());
    }
}
