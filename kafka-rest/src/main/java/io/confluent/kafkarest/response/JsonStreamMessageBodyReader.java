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
import io.confluent.kafkarest.KafkaRestConfig;
import io.confluent.kafkarest.response.JsonStream.SizeLimitEntityStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.ext.MessageBodyReader;

/** A {@link MessageBodyReader} for {@link JsonStream}. */
public final class JsonStreamMessageBodyReader implements MessageBodyReader<JsonStream<?>> {
  private final ObjectMapper objectMapper;
  private final KafkaRestConfig config;

  public JsonStreamMessageBodyReader(ObjectMapper objectMapper, KafkaRestConfig config) {
    this.objectMapper = requireNonNull(objectMapper);
    this.config = requireNonNull(config);
  }

  @Override
  public boolean isReadable(
      Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType) {
    return JsonStream.class.equals(type)
        && MediaType.APPLICATION_JSON_TYPE.getType().equalsIgnoreCase(mediaType.getType())
        && MediaType.APPLICATION_JSON_TYPE.getSubtype().equalsIgnoreCase(mediaType.getSubtype());
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

    // We know that the media type is 'application/json' but there might be an optional charset too
    if (mediaType.getParameters() != null) {
      String charset = mediaType.getParameters().get(MediaType.CHARSET_PARAMETER);
      if (charset != null) {
        if (!charset.equalsIgnoreCase("UTF-8") && !charset.equalsIgnoreCase("ISO-8859-1")) {
          throw new WebApplicationException(
              "Unsupported charset in Content-Type header. Supports \"UTF-8\" and \"ISO-8859-1\".",
              Status.BAD_REQUEST);
        }
      }
    }

    JavaType wrappedType = objectMapper.constructType(genericType).containedType(0);
    SizeLimitEntityStream wrappedInputStream =
        (config.getProduceRequestSizeLimitMaxBytesConfig() > 0)
            ? new SizeLimitEntityStream(
                entityStream, config.getProduceRequestSizeLimitMaxBytesConfig())
            : null;
    return new JsonStream<>(
        () -> {
          try {
            // JsonParser needs to be instantiated lazily because it eagerly reads in a few bytes
            // from the entityStream. If this initial bootstrapping doesn't work, and this is not
            // done lazily, it can create problems with releasing request scoped resources. See
            // KREST-5830 for context.
            JsonParser parser =
                objectMapper.createParser(
                    wrappedInputStream == null ? entityStream : wrappedInputStream);
            return objectMapper.readValues(parser, wrappedType);
          } catch (IOException e) {
            throw new BadRequestException("Unexpected error while starting JSON stream: ", e);
          }
        },
        wrappedInputStream);
  }
}
