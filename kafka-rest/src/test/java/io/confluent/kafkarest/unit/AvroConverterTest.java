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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.TextNode;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericEnumSymbol;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.util.Utf8;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import io.confluent.kafkarest.TestUtils;
import io.confluent.kafkarest.converters.AvroConverter;
import io.confluent.kafkarest.converters.ConversionException;
import io.confluent.kafkarest.entities.EntityUtils;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class AvroConverterTest {

  private static final Schema.Parser parser = new Schema.Parser();

  private static final Schema recordSchema = new Schema.Parser().parse(
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
      + "     {\"name\": \"string\", \"type\": \"string\", \"aliases\": [\"string_alias\"]},\n"
      + "     {\"name\": \"null_default\", \"type\": \"null\", \"default\": null},\n"
      + "     {\"name\": \"boolean_default\", \"type\": \"boolean\", \"default\": false},\n"
      + "     {\"name\": \"int_default\", \"type\": \"int\", \"default\": 24},\n"
      + "     {\"name\": \"long_default\", \"type\": \"long\", \"default\": 4000000000},\n"
      + "     {\"name\": \"float_default\", \"type\": \"float\", \"default\": 12.3},\n"
      + "     {\"name\": \"double_default\", \"type\": \"double\", \"default\": 23.2},\n"
      + "     {\"name\": \"bytes_default\", \"type\": \"bytes\", \"default\": \"bytes\"},\n"
      + "     {\"name\": \"string_default\", \"type\": \"string\", \"default\": "
      + "\"default string\"}\n"
      + "]\n"
      + "}"
  );

  private static final Schema arraySchema = new Schema.Parser().parse(
      "{\"namespace\": \"namespace\",\n"
      + " \"type\": \"array\",\n"
      + " \"name\": \"test\",\n"
      + " \"items\": \"string\"\n"
      + "}"
  );

  private static final Schema mapSchema = new Schema.Parser().parse(
      "{\"namespace\": \"namespace\",\n"
      + " \"type\": \"map\",\n"
      + " \"name\": \"test\",\n"
      + " \"values\": \"string\"\n"
      + "}"
  );

  private static final Schema unionSchema = new Schema.Parser().parse(
      "{\"type\": \"record\",\n"
      + " \"name\": \"test\",\n"
      + " \"fields\": [\n"
      + "     {\"name\": \"union\", \"type\": [\"string\", \"int\"]}\n"
      + "]}");


  private static final Schema enumSchema = new Schema.Parser().parse(
      "{ \"type\": \"enum\",\n"
      + "  \"name\": \"Suit\",\n"
      + "  \"symbols\" : [\"SPADES\", \"HEARTS\", \"DIAMONDS\", \"CLUBS\"]\n"
      + "}"
  );

  @Test
  public void testPrimitiveTypesToAvro() {
    Object result = new AvroConverter().toObject(null, createPrimitiveSchema("null"));
    assertTrue(result == null);

    result = new AvroConverter().toObject(TestUtils.jsonTree("true"), createPrimitiveSchema("boolean"));
    assertEquals(true, result);
    result = new AvroConverter().toObject(TestUtils.jsonTree("false"), createPrimitiveSchema("boolean"));
    assertEquals(false, result);

    result = new AvroConverter().toObject(TestUtils.jsonTree("12"), createPrimitiveSchema("int"));
    assertTrue(result instanceof Integer);
    assertEquals(12, result);

    result = new AvroConverter().toObject(TestUtils.jsonTree("12"), createPrimitiveSchema("long"));
    assertTrue(result instanceof Long);
    assertEquals(12L, result);
    result = new AvroConverter().toObject(TestUtils.jsonTree("5000000000"), createPrimitiveSchema("long"));
    assertTrue(result instanceof Long);
    assertEquals(5000000000L, result);

    result = new AvroConverter().toObject(TestUtils.jsonTree("23.2"), createPrimitiveSchema("float"));
    assertTrue(result instanceof Float);
    assertEquals(23.2f, result);
    result = new AvroConverter().toObject(TestUtils.jsonTree("23"), createPrimitiveSchema("float"));
    assertTrue(result instanceof Float);
    assertEquals(23.0f, result);

    result = new AvroConverter().toObject(TestUtils.jsonTree("23.2"), createPrimitiveSchema("double"));
    assertTrue(result instanceof Double);
    assertEquals(23.2, result);
    result = new AvroConverter().toObject(TestUtils.jsonTree("23"), createPrimitiveSchema("double"));
    assertTrue(result instanceof Double);
    assertEquals(23.0, result);

    // We can test bytes simply using simple ASCII string since the translation is direct in that
    // case
    result = new AvroConverter().toObject(new TextNode("hello"), createPrimitiveSchema("bytes"));
    assertTrue(result instanceof ByteBuffer);
    assertEquals(EntityUtils.encodeBase64Binary("hello".getBytes()),
                 EntityUtils.encodeBase64Binary(((ByteBuffer) result).array()));

    result = new AvroConverter().toObject(TestUtils.jsonTree("\"a string\""),
                                  createPrimitiveSchema("string"));
    assertTrue(result instanceof Utf8);
    assertEquals(new Utf8("a string"), result);
  }

  @Test
  public void testPrimitiveTypeToAvroSchemaMismatches() {
    expectConversionException(TestUtils.jsonTree("12"), createPrimitiveSchema("null"));

    expectConversionException(TestUtils.jsonTree("12"), createPrimitiveSchema("boolean"));

    expectConversionException(TestUtils.jsonTree("false"), createPrimitiveSchema("int"));
    // Note that we don't test real numbers => int because JsonDecoder permits this and removes
    // the decimal part
    expectConversionException(TestUtils.jsonTree("5000000000"), createPrimitiveSchema("int"));

    expectConversionException(TestUtils.jsonTree("false"), createPrimitiveSchema("long"));
    // Note that we don't test real numbers => long because JsonDecoder permits this and removes
    // the decimal part

    expectConversionException(TestUtils.jsonTree("false"), createPrimitiveSchema("float"));

    expectConversionException(TestUtils.jsonTree("false"), createPrimitiveSchema("double"));

    expectConversionException(TestUtils.jsonTree("false"), createPrimitiveSchema("bytes"));

    expectConversionException(TestUtils.jsonTree("false"), createPrimitiveSchema("string"));
  }

  @Test
  public void testRecordToAvro() {
    String json = "{\n"
                  + "    \"null\": null,\n"
                  + "    \"boolean\": true,\n"
                  + "    \"int\": 12,\n"
                  + "    \"long\": 5000000000,\n"
                  + "    \"float\": 23.4,\n"
                  + "    \"double\": 800.25,\n"
                  + "    \"bytes\": \"hello\",\n"
                  + "    \"string\": \"string\",\n"
                  + "    \"null_default\": null,\n"
                  + "    \"boolean_default\": false,\n"
                  + "    \"int_default\": 24,\n"
                  + "    \"long_default\": 4000000000,\n"
                  + "    \"float_default\": 12.3,\n"
                  + "    \"double_default\": 23.2,\n"
                  + "    \"bytes_default\": \"bytes\",\n"
                  + "    \"string_default\": \"default\"\n"
                  + "}";

    Object result = new AvroConverter().toObject(TestUtils.jsonTree(json), recordSchema);
    assertTrue(result instanceof GenericRecord);
    GenericRecord resultRecord = (GenericRecord) result;
    assertEquals(null, resultRecord.get("null"));
    assertEquals(true, resultRecord.get("boolean"));
    assertEquals(12, resultRecord.get("int"));
    assertEquals(5000000000L, resultRecord.get("long"));
    assertEquals(23.4f, resultRecord.get("float"));
    assertEquals(800.25, resultRecord.get("double"));
    assertEquals(EntityUtils.encodeBase64Binary("hello".getBytes()),
                 EntityUtils.encodeBase64Binary(((ByteBuffer) resultRecord.get("bytes")).array()));
    assertEquals("string", resultRecord.get("string").toString());
    // Nothing to check with default values, just want to make sure an exception wasn't thrown
    // when they values weren't specified for their fields.
  }

  @Test
  public void testArrayToAvro() {
    String json = "[\"one\", \"two\", \"three\"]";

    Object result = new AvroConverter().toObject(TestUtils.jsonTree(json), arraySchema);
    assertTrue(result instanceof GenericArray);
    assertArrayEquals(new Utf8[]{new Utf8("one"), new Utf8("two"), new Utf8("three")},
                      ((GenericArray) result).toArray());
  }

  @Test
  public void testMapToAvro() {
    String json = "{\"first\": \"one\", \"second\": \"two\"}";

    Object result = new AvroConverter().toObject(TestUtils.jsonTree(json), mapSchema);
    assertTrue(result instanceof Map);
    assertEquals(2, ((Map<String, Object>) result).size());
  }

  @Test
  public void testUnionToAvro() {
    Object result = new AvroConverter().toObject(
        TestUtils.jsonTree("{\"union\":{\"string\":\"test string\"}}"),
        unionSchema);
    Object foo = ((GenericRecord) result).get("union");
    assertTrue(((GenericRecord) result).get("union") instanceof Utf8);

    result = new AvroConverter().toObject(TestUtils.jsonTree("{\"union\":{\"int\":12}}"), unionSchema);
    assertTrue(((GenericRecord) result).get("union") instanceof Integer);

    try {
      new AvroConverter().toObject(TestUtils.jsonTree("12.4"), unionSchema);
      fail("Trying to convert floating point number to union(string,int) schema should fail");
    } catch (ConversionException e) {
      // expected
    }
  }

  @Test
  public void testEnumToAvro() {
    Object result = new AvroConverter().toObject(TestUtils.jsonTree("\"SPADES\""), enumSchema);
    assertTrue(result instanceof GenericEnumSymbol);

    // There's no failure case here because the only failure mode is passing in non-string data.
    // Even if they put in an invalid symbol name, the exception won't be thrown until
    // serialization.
  }


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
    GenericRecord data = new GenericRecordBuilder(recordSchema)
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
    GenericData.Array<String>
        data = new GenericData.Array(arraySchema, Arrays.asList("one", "two", "three"));
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
    AvroConverter.JsonNodeAndSize result
        = new AvroConverter().toJson(new GenericData.EnumSymbol(enumSchema, "SPADES"));
    assertTrue(result.getSize() > 0);
    assertTrue(result.getJson().isTextual());
    assertEquals("SPADES", result.getJson().textValue());
  }


  private static void expectConversionException(JsonNode obj, Schema schema) {
    try {
      new AvroConverter().toObject(obj, schema);
      fail("Expected conversion of " + (obj == null ? "null" : obj.toString())
           + " to schema " + schema.toString() + " to fail");
    } catch (ConversionException e) {
      // Expected
    }
  }

  private static void expectConversionException(Object obj) {
    try {
      new AvroConverter().toJson(obj);
      fail("Expected conversion of "
           + (obj == null ? "null" : (obj.toString() + " (" + obj.getClass().getName() + ")"))
           + " to fail");
    } catch (ConversionException e) {
      // Expected
    }
  }

  private static Schema createPrimitiveSchema(String type) {
    String schemaString = String.format("{\"type\" : \"%s\"}", type);
    return parser.parse(schemaString);
  }
}
