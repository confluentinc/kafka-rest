/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.kafkarest.entities;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

@JsonSerialize(using = ForwardHeader.HeaderSerializer.class)
@JsonDeserialize(using = ForwardHeader.HeaderDeserializer.class)
public final class ForwardHeader {
  private static final String NULL_KEY_MESSAGE = "Null header keys are not permitted";
  public final String key;
  public final byte[] value;

  public ForwardHeader(Header header) {
    this(header.key(), header.value());
  }

  public ForwardHeader(String key, byte[] value) {
    this.key = Objects.requireNonNull(key, NULL_KEY_MESSAGE);
    this.value = value;
  }

  public ForwardHeader(String key, String value) {
    this(key, Objects.requireNonNull(
        value, "Null header string value").getBytes(StandardCharsets.UTF_8));
  }

  public Header toHeader() {
    return new RecordHeader(key, value);
  }

  protected static final class HeaderDeserializer extends StdDeserializer<ForwardHeader> {

    public HeaderDeserializer() {
      super(ForwardHeader.class);
    }

    protected HeaderDeserializer(Class<?> vc) {
      super(vc);
    }

    @Override
    public ForwardHeader deserialize(JsonParser p, DeserializationContext ctx) throws IOException {
      String key = p.nextFieldName();
      String value = p.nextTextValue();
      //noinspection StatementWithEmptyBody
      while (p.nextToken() != JsonToken.END_OBJECT) {
      }
      if (value != null) {
        return new ForwardHeader(key, value.getBytes(StandardCharsets.UTF_8));
      }
      return null;
    }
  }

  protected static final class HeaderSerializer extends StdSerializer<ForwardHeader> {

    public HeaderSerializer() {
      super(ForwardHeader.class);
    }

    protected HeaderSerializer(Class<ForwardHeader> t) {
      super(t);
    }

    @Override
    public void serialize(ForwardHeader value, JsonGenerator gen, SerializerProvider provider)
        throws IOException {
      if (value != null && value.value != null) {
        gen.writeStartObject();
        gen.writeStringField(value.key, new String(value.value, StandardCharsets.UTF_8));
        gen.writeEndObject();
      }
    }
  }
}
