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

package io.confluent.kafkarest.unit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import io.confluent.kafkarest.converters.AvroConverter;
import io.confluent.kafkarest.converters.ConversionException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.junit.jupiter.api.Test;

public class AvroConverterTest {

  private static final Schema recordSchema =
      new Schema.Parser()
          .parse(
              "{\"namespace\": \"namespace\",\n"
                  + " \"type\": \"record\",\n"
                  + " \"name\": \"test\",\n"
                  + " \"fields\": [\n"
                  + "     {\"name\": \"null\", \"type\": \"null\"},\n"
                  + "     {\"name\": \"boolean\", \"type\": \"boolean\"},\n"
                  + "     {\"name\": \"int\", \"type\": \"int\"},\n"
                  + "     {\"name\": \"long\", \"type\": \"long\"},\n"
                  + "     {\"name\": \"float\", \"type\": \"float\"},\n"
                  + "     {\"name\": \"double\", \"type\": \"double\"},\n"
                  + "     {\"name\": \"bytes\", \"type\": \"bytes\"},\n"
                  + "     {\"name\": \"string\", \"type\": \"string\", \"aliases\":"
                  + " [\"string_alias\"]},\n"
                  + "     {\"name\": \"null_default\", \"type\": \"null\", \"default\": null},\n"
                  + "     {\"name\": \"boolean_default\", \"type\": \"boolean\", \"default\":"
                  + " false},\n"
                  + "     {\"name\": \"int_default\", \"type\": \"int\", \"default\": 24},\n"
                  + "     {\"name\": \"long_default\", \"type\": \"long\", \"default\":"
                  + " 4000000000},\n"
                  + "     {\"name\": \"float_default\", \"type\": \"float\", \"default\": 12.3},\n"
                  + "     {\"name\": \"double_default\", \"type\": \"double\", \"default\":"
                  + " 23.2},\n"
                  + "     {\"name\": \"bytes_default\", \"type\": \"bytes\", \"default\":"
                  + " \"bytes\"},\n"
                  + "     {\"name\": \"string_default\", \"type\": \"string\", \"default\":"
                  + " \"default string\"}\n"
                  + "]\n"
                  + "}");

  private static final Schema arraySchema =
      new Schema.Parser()
          .parse(
              "{\"namespace\": \"namespace\",\n"
                  + " \"type\": \"array\",\n"
                  + " \"name\": \"test\",\n"
                  + " \"items\": \"string\"\n"
                  + "}");

  private static final Schema enumSchema =
      new Schema.Parser()
          .parse(
              "{ \"type\": \"enum\",\n"
                  + "  \"name\": \"Suit\",\n"
                  + "  \"symbols\" : [\"SPADES\", \"HEARTS\", \"DIAMONDS\", \"CLUBS\"]\n"
                  + "}");

  @Test
  public void testPrimitiveTypesToJson() {
    AvroConverter.JsonNodeAndSize result = new AvroConverter().toJson((int) 0);
    assertTrue(result.getJson().isNumber());
    assertTrue(result.getSize() > 0);

    result = new AvroConverter().toJson((long) 0);
    assertTrue(result.getJson().isNumber());

    result = new AvroConverter().toJson(0.1f);
    assertTrue(result.getJson().isNumber());

    result = new AvroConverter().toJson(0.1);
    assertTrue(result.getJson().isNumber());

    result = new AvroConverter().toJson(true);
    assertTrue(result.getJson().isBoolean());

    // "Primitive" here refers to Avro primitive types, which are returned as standalone objects,
    // which can't have attached schemas. This includes, for example, Strings and byte[] even
    // though they are not Java primitives

    result = new AvroConverter().toJson("abcdefg");
    assertTrue(result.getJson().isTextual());
    assertEquals("abcdefg", result.getJson().textValue());

    result = new AvroConverter().toJson(ByteBuffer.wrap("hello".getBytes()));
    assertTrue(result.getJson().isTextual());
    // Was generated from a string, so the Avro encoding should be equivalent to the string
    assertEquals("hello", result.getJson().textValue());
  }

  @Test
  public void testUnsupportedJavaPrimitivesToJson() {
    expectConversionException((byte) 0);
    expectConversionException((char) 0);
    expectConversionException((short) 0);
  }

  @Test
  public void testRecordToJson() {
    GenericRecord data =
        new GenericRecordBuilder(recordSchema)
            .set("null", null)
            .set("boolean", true)
            .set("int", 12)
            .set("long", 5000000000L)
            .set("float", 23.4f)
            .set("double", 800.25)
            .set("bytes", ByteBuffer.wrap("bytes".getBytes()))
            .set("string", "string")
            .build();

    AvroConverter.JsonNodeAndSize result = new AvroConverter().toJson(data);
    assertTrue(result.getSize() > 0);
    assertTrue(result.getJson().isObject());
    assertTrue(result.getJson().get("null").isNull());
    assertTrue(result.getJson().get("boolean").isBoolean());
    assertEquals(true, result.getJson().get("boolean").booleanValue());
    assertTrue(result.getJson().get("int").isIntegralNumber());
    assertEquals(12, result.getJson().get("int").intValue());
    assertTrue(result.getJson().get("long").isIntegralNumber());
    assertEquals(5000000000L, result.getJson().get("long").longValue());
    assertTrue(result.getJson().get("float").isFloatingPointNumber());
    assertEquals(23.4f, result.getJson().get("float").floatValue(), 0.1);
    assertTrue(result.getJson().get("double").isFloatingPointNumber());
    assertEquals(800.25, result.getJson().get("double").doubleValue(), 0.01);
    assertTrue(result.getJson().get("bytes").isTextual());
    // The bytes value was created from an ASCII string, so Avro's encoding should just give that
    // string back to us in the JSON-serialized version
    assertEquals("bytes", result.getJson().get("bytes").textValue());
    assertTrue(result.getJson().get("string").isTextual());
    assertEquals("string", result.getJson().get("string").textValue());
  }

  @Test
  public void testArrayToJson() {
    GenericData.Array<String> data =
        new GenericData.Array(arraySchema, Arrays.asList("one", "two", "three"));
    AvroConverter.JsonNodeAndSize result = new AvroConverter().toJson(data);
    assertTrue(result.getSize() > 0);

    assertTrue(result.getJson().isArray());
    assertEquals(3, result.getJson().size());
    assertEquals(JsonNodeFactory.instance.textNode("one"), result.getJson().get(0));
    assertEquals(JsonNodeFactory.instance.textNode("two"), result.getJson().get(1));
    assertEquals(JsonNodeFactory.instance.textNode("three"), result.getJson().get(2));
  }

  @Test
  public void testMapToJson() {
    Map<String, Object> data = new HashMap<String, Object>();
    data.put("first", "one");
    data.put("second", "two");
    AvroConverter.JsonNodeAndSize result = new AvroConverter().toJson(data);
    assertTrue(result.getSize() > 0);

    assertTrue(result.getJson().isObject());
    assertEquals(2, result.getJson().size());
    assertNotNull(result.getJson().get("first"));
    assertEquals("one", result.getJson().get("first").asText());
    assertNotNull(result.getJson().get("second"));
    assertEquals("two", result.getJson().get("second").asText());
  }

  @Test
  public void testEnumToJson() {
    AvroConverter.JsonNodeAndSize result =
        new AvroConverter().toJson(new GenericData.EnumSymbol(enumSchema, "SPADES"));
    assertTrue(result.getSize() > 0);
    assertTrue(result.getJson().isTextual());
    assertEquals("SPADES", result.getJson().textValue());
  }

  private static void expectConversionException(Object obj) {
    try {
      new AvroConverter().toJson(obj);
      fail(
          "Expected conversion of "
              + (obj == null ? "null" : (obj.toString() + " (" + obj.getClass().getName() + ")"))
              + " to fail");
    } catch (ConversionException e) {
      // Expected
    }
  }
}
