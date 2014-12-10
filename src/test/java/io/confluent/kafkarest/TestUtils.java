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

import io.confluent.rest.entities.ErrorMessage;

import javax.ws.rs.core.Response;

import static org.junit.Assert.assertEquals;

public class TestUtils {
    // Media type collections that should be tested together (i.e. expect the same raw output). The expected output
    // format is included so these lists can include weighted Accept headers.
    public static final RequestMediaType[] V1_ACCEPT_MEDIATYPES = {
            // Single type in Accept header
            new RequestMediaType(Versions.KAFKA_V1_JSON, Versions.KAFKA_V1_JSON),
            new RequestMediaType(Versions.KAFKA_DEFAULT_JSON, Versions.KAFKA_DEFAULT_JSON),
            new RequestMediaType(Versions.JSON, Versions.JSON),
            // Weighted options in Accept header should select the highest weighted option
            new RequestMediaType(Versions.KAFKA_V1_JSON_WEIGHTED + ", " + Versions.KAFKA_DEFAULT_JSON_WEIGHTED + ", " + Versions.JSON, Versions.KAFKA_V1_JSON),
            new RequestMediaType(Versions.KAFKA_V1_JSON + "; q=0.8, " + Versions.KAFKA_DEFAULT_JSON + "; q=0.9, " + Versions.JSON + "; q=0.7", Versions.KAFKA_DEFAULT_JSON),
            new RequestMediaType(Versions.KAFKA_V1_JSON + "; q=0.8, " + Versions.KAFKA_DEFAULT_JSON + "; q=0.7, " + Versions.JSON + "; q=0.9", Versions.JSON),
            // No accept header, should use most specific default media type
            new RequestMediaType(null, Versions.KAFKA_MOST_SPECIFIC_DEFAULT)
    };

    // Response content types we should never allow to be produced
    public static final String[] V1_INVALID_MEDIATYPES = {
            "text/plain",
            "application/octet-stream"
    };

    public static final String[] V1_REQUEST_ENTITY_TYPES = {
        Versions.KAFKA_V1_JSON, Versions.KAFKA_DEFAULT_JSON, Versions.JSON, Versions.GENERIC_REQUEST
    };

    // Request content types we'll always ignore
    public static final String[] V1_INVALID_REQUEST_MEDIATYPES = {
            "text/plain"
    };

    /**
     * Asserts that the response received an HTTP 200 status code, as well as some optional requirements such as the Content-Type.
     */
    public static void assertOKResponse(Response rawResponse, String mediatype) {
        assertEquals(Response.Status.OK.getStatusCode(), rawResponse.getStatus());
        assertEquals(mediatype, rawResponse.getMediaType().toString());
    }

    /**
     * Asserts that the correct HTTP status code was set for the error and that a generic structured response is returned.
     * This requires a custom message to check against since they should always be provided or explicitly specify the
     * default.
     */
    public static void assertErrorResponse(Response.StatusType status, Response rawResponse, String msg, String mediatype) {
        assertEquals(status.getStatusCode(), rawResponse.getStatus());

        // Successful deletion's return no content, so we shouldn't try to decode their entities
        if (status.equals(Response.Status.NO_CONTENT)) return;

        assertEquals(mediatype, rawResponse.getMediaType().toString());

        ErrorMessage response = rawResponse.readEntity(ErrorMessage.class);
        assertEquals(status.getStatusCode(), response.getErrorCode());
        if (msg != null)
            assertEquals(msg, response.getMessage());
    }



    public static class RequestMediaType {
        public final String header;
        public final String expected;

        public RequestMediaType(String header, String expected) {
            this.header = header;
            this.expected = expected;
        }
    }
}
