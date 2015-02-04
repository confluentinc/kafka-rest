/**
 * Copyright 2015 Confluent Inc.
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
 **/

package io.confluent.kafkarest.unit;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.TextNode;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericEnumSymbol;
import org.apache.avro.generic.GenericRecord;
import org.junit.Test;

import java.util.Arrays;
import java.util.Map;

import io.confluent.kafkarest.TestUtils;
import io.confluent.kafkarest.converters.AvroConverter;
import io.confluent.kafkarest.converters.ConversionException;
import io.confluent.kafkarest.entities.EntityUtils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class AvroConverterTest {

  private static final Schema.Parser parser = new Schema.Parser();

  @Test
  public void testPrimitiveTypes() {
    Object result = AvroConverter.toAvro(null, createPrimitiveSchema("null"));
    assertTrue(result == null);

    result = AvroConverter.toAvro(TestUtils.jsonTree("true"), createPrimitiveSchema("boolean"));
    assertEquals(true, result);
    result = AvroConverter.toAvro(TestUtils.jsonTree("false"), createPrimitiveSchema("boolean"));
    assertEquals(false, result);

    result = AvroConverter.toAvro(TestUtils.jsonTree("12"), createPrimitiveSchema("int"));
    assertTrue(result instanceof Integer);
    assertEquals(12, result);

    result = AvroConverter.toAvro(TestUtils.jsonTree("12"), createPrimitiveSchema("long"));
    assertTrue(result instanceof Long);
    assertEquals(12L, result);
    result = AvroConverter.toAvro(TestUtils.jsonTree("5000000000"), createPrimitiveSchema("long"));
    assertTrue(result instanceof Long);
    assertEquals(5000000000L, result);

    result = AvroConverter.toAvro(TestUtils.jsonTree("23.2"), createPrimitiveSchema("float"));
    assertTrue(result instanceof Float);
    assertEquals(23.2f, result);
    result = AvroConverter.toAvro(TestUtils.jsonTree("23"), createPrimitiveSchema("float"));
    assertTrue(result instanceof Float);
    assertEquals(23.0f, result);

    result = AvroConverter.toAvro(TestUtils.jsonTree("23.2"), createPrimitiveSchema("double"));
    assertTrue(result instanceof Double);
    assertEquals(23.2, result);
    result = AvroConverter.toAvro(TestUtils.jsonTree("23"), createPrimitiveSchema("double"));
    assertTrue(result instanceof Double);
    assertEquals(23.0, result);

    result = AvroConverter.toAvro(new TextNode(EntityUtils.encodeBase64Binary("hello".getBytes())),
                                  createPrimitiveSchema("bytes"));
    assertTrue(result instanceof byte[]);
    assertEquals(EntityUtils.encodeBase64Binary("hello".getBytes()), EntityUtils
        .encodeBase64Binary((byte[]) result));

    result = AvroConverter.toAvro(TestUtils.jsonTree("\"a string\""),
                                  createPrimitiveSchema("string"));
    assertTrue(result instanceof String);
    assertEquals("a string", result);
  }

  @Test
  public void testPrimitiveTypeSchemaMismatches() {
    expectConversionException(TestUtils.jsonTree("12"), createPrimitiveSchema("null"));

    expectConversionException(TestUtils.jsonTree("12"), createPrimitiveSchema("boolean"));

    expectConversionException(TestUtils.jsonTree("false"), createPrimitiveSchema("int"));
    expectConversionException(TestUtils.jsonTree("12.1"), createPrimitiveSchema("int"));
    expectConversionException(TestUtils.jsonTree("5000000000"), createPrimitiveSchema("int"));

    expectConversionException(TestUtils.jsonTree("false"), createPrimitiveSchema("long"));
    expectConversionException(TestUtils.jsonTree("12.1"), createPrimitiveSchema("long"));

    expectConversionException(TestUtils.jsonTree("false"), createPrimitiveSchema("float"));

    expectConversionException(TestUtils.jsonTree("false"), createPrimitiveSchema("double"));

    expectConversionException(TestUtils.jsonTree("false"), createPrimitiveSchema("bytes"));

    expectConversionException(TestUtils.jsonTree("false"), createPrimitiveSchema("string"));
  }

  @Test
  public void testRecord() {
    Schema schema = new Schema.Parser().parse(
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
        + "     {\"name\": \"bytes_default\", \"type\": \"bytes\", \"default\": \"ZGVmYXVsdA==\"},\n"
        + "     {\"name\": \"string_default\", \"type\": \"string\", \"default\": "
        + "\"default string\"}\n"
        + "]\n"
        + "}"
    );

    String json = "{\n"
                  + "    \"null\": null,\n"
                  + "    \"boolean\": true,\n"
                  + "    \"int\": 12,\n"
                  + "    \"long\": 5000000000,\n"
                  + "    \"float\": 23.4,\n"
                  + "    \"double\": 800.25,\n"
                  + "    \"bytes\": \"Ynl0ZXM=\",\n"
                  + "    \"string_alias\": \"string\"\n" // tests aliasing
                  + "}";

    Object result = AvroConverter.toAvro(TestUtils.jsonTree(json), schema);
    assertTrue(result instanceof GenericRecord);
    GenericRecord resultRecord = (GenericRecord) result;
    assertEquals(null, resultRecord.get("null"));
    assertEquals(true, resultRecord.get("boolean"));
    assertEquals(12, resultRecord.get("int"));
    assertEquals(5000000000L, resultRecord.get("long"));
    assertEquals(23.4f, resultRecord.get("float"));
    assertEquals(800.25, resultRecord.get("double"));
    assertEquals("Ynl0ZXM=", EntityUtils.encodeBase64Binary((byte[]) resultRecord.get("bytes")));
    assertEquals("string", resultRecord.get("string"));
    // Nothing to check with default values, just want to make sure an exception wasn't thrown
    // when they values weren't specified for their fields.
  }

  @Test
  public void testArray() {
    Schema schema = new Schema.Parser().parse(
        "{\"namespace\": \"namespace\",\n"
        + " \"type\": \"array\",\n"
        + " \"name\": \"test\",\n"
        + " \"items\": \"string\"\n"
        + "}"
    );

    String json = "[\"one\", \"two\", \"three\"]";

    Object result = AvroConverter.toAvro(TestUtils.jsonTree(json), schema);
    assertTrue(result instanceof GenericArray);
    assertEquals(Arrays.asList("one", "two", "three"), result);
  }

  @Test
  public void testMap() {
    Schema schema = new Schema.Parser().parse(
        "{\"namespace\": \"namespace\",\n"
        + " \"type\": \"map\",\n"
        + " \"name\": \"test\",\n"
        + " \"values\": \"string\"\n"
        + "}"
    );

    String json = "{\"first\": \"one\", \"second\": \"two\"}";

    Object result = AvroConverter.toAvro(TestUtils.jsonTree(json), schema);
    assertTrue(result instanceof Map);
    assertEquals(2, ((Map<String, Object>) result).size());
  }

  @Test
  public void testUnion() {
    Schema schema = new Schema.Parser().parse(
        "{\"type\": \"record\",\n"
        + " \"name\": \"test\",\n"
        + " \"fields\": [\n"
        + "     {\"name\": \"union\", \"type\": [\"string\", \"int\"]}\n"
        + "]}");

    Object result = AvroConverter.toAvro(TestUtils.jsonTree("{\"union\":\"test string\"}"), schema);
    assertTrue(((GenericRecord) result).get("union") instanceof String);

    result = AvroConverter.toAvro(TestUtils.jsonTree("{\"union\":12}"), schema);
    assertTrue(((GenericRecord) result).get("union") instanceof Integer);

    try {
      AvroConverter.toAvro(TestUtils.jsonTree("12.4"), schema);
      fail("Trying to convert floating point number to union(string,int) schema should fail");
    } catch (ConversionException e) {
      // expected
    }
  }

  @Test
  public void testEnum() {
    Schema schema = new Schema.Parser().parse(
        "{ \"type\": \"enum\",\n"
        + "  \"name\": \"Suit\",\n"
        + "  \"symbols\" : [\"SPADES\", \"HEARTS\", \"DIAMONDS\", \"CLUBS\"]\n"
        + "}"
    );

    Object result = AvroConverter.toAvro(TestUtils.jsonTree("\"SPADES\""), schema);
    assertTrue(result instanceof GenericEnumSymbol);

    // There's no failure case here because the only failure mode is passing in non-string data.
    // Even if they put in an invalid symbol name, the exception won't be thrown until
    // serialization.
  }

  private static void expectConversionException(JsonNode obj, Schema schema) {
    try {
      AvroConverter.toAvro(obj, schema);
      fail("Expected conversion of " + (obj == null ? "null" : obj.toString())
           + " to schema " + schema.toString() + " to fail");
    } catch (ConversionException e) {
      // Expected
    }
  }

  private static Schema createPrimitiveSchema(String type) {
    String schemaString = String.format("{\"type\" : \"%s\"}", type);
    return parser.parse(schemaString);
  }
}
