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

package io.confluent.kafkarest.response;

import static java.util.Objects.requireNonNull;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.InputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.MessageBodyReader;

/** A {@link MessageBodyReader} for {@link JsonStream}. */
public final class JsonStreamMessageBodyReader implements MessageBodyReader<JsonStream<?>> {
  private final ObjectMapper objectMapper;

  public JsonStreamMessageBodyReader(ObjectMapper objectMapper) {
    this.objectMapper = requireNonNull(objectMapper);
  }

  @Override
  public boolean isReadable(
      Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType) {
    return JsonStream.class.equals(type) && MediaType.APPLICATION_JSON_TYPE.equals(mediaType);
  }

  @Override
  public JsonStream<?> readFrom(
      Class<JsonStream<?>> type,
      Type genericType,
      Annotation[] annotations,
      MediaType mediaType,
      MultivaluedMap<String, String> httpHeaders,
      InputStream entityStream)
      throws WebApplicationException {
    JavaType wrappedType = objectMapper.constructType(genericType).containedType(0);
    return new JsonStream<>(
        () -> {
          try {
            // JsonParser needs to be instantiated lazily because it eagerly reads in a few bytes
            // from the entityStream. If this initial bootstrapping doesn't work, and this is not
            // done lazily, it can create problems with releasing request scoped resources. See
            // KREST-5830 for context.
            JsonParser parser = objectMapper.createParser(entityStream);
            return objectMapper.readValues(parser, wrappedType);
          } catch (IOException e) {
            throw new BadRequestException("Unexpected error while starting JSON stream: ", e);
          }
        });
  }
}
