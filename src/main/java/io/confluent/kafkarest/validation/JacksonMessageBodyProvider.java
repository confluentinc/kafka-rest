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
package io.confluent.kafkarest.validation;

import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.jaxrs.json.JacksonJaxbJsonProvider;

import javax.validation.ConstraintViolationException;
import javax.ws.rs.Consumes;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.Provider;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;

/**
 * Jackson provider that handles some additional exceptions. This allows additional processing
 * and validation of entities during parsing that don't fit well with the standard validation framework (e.g.
 * decoding of encoded fields to avoid storing the original and repeated decoding if the field is accessed
 * multiple times).
 */
@Provider
@Consumes(MediaType.WILDCARD) // NOTE: required to support "non-standard" JSON variants
@Produces(MediaType.WILDCARD)
public class JacksonMessageBodyProvider extends JacksonJaxbJsonProvider {

    public JacksonMessageBodyProvider() {
        setMapper(new ObjectMapper());
    }

    @Override
    public Object readFrom(Class<Object> type,
                           Type genericType,
                           Annotation[] annotations,
                           MediaType mediaType,
                           MultivaluedMap<String, String> httpHeaders,
                           InputStream entityStream) throws IOException {
        try {
            return super.readFrom(type, genericType, annotations, mediaType, httpHeaders, entityStream);
        } catch (JsonMappingException e) {
            // This needs to handle 2 JSON parsing error cases. Normally you would expect to see a JsonMappingException
            // because the data couldn't be parsed, but it can also occur when the raw JSON is valid and satisfies the
            // validation constraint annotations, but an exception is thrown by the entity
            // during construction. In the former case, we want to return a 400 (Bad Request), in the latter a 422
            // (Unprocessable Entity) with a useful error message. We don't want to expose just any exception message
            // via the API, so this code specifically detects ConstraintViolationExceptions that were thrown *after*
            // the normal validation checks, i.e. when the entity Java object was being constructed.
            Throwable cause = e.getCause();
            if (cause instanceof ConstraintViolationException)
                throw (ConstraintViolationException)cause;
            throw e;
        }
    }
}
