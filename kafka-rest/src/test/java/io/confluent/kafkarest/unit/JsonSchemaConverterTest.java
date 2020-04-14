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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.NumericNode;
import com.fasterxml.jackson.databind.node.TextNode;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.json.JsonSchemaUtils;
import io.confluent.kafkarest.TestUtils;
import io.confluent.kafkarest.converters.ConversionException;
import io.confluent.kafkarest.converters.JsonSchemaConverter;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class JsonSchemaConverterTest {

  private static final String recordSchemaString =
      "{\"properties\": {\n"
      + "     \"null\": {\"type\": \"null\"},\n"
      + "     \"boolean\": {\"type\": \"boolean\"},\n"
      + "     \"number\": {\"type\": \"number\"},\n"
      + "     \"string\": {\"type\": \"string\"}\n"
      + "  },\n"
      + "  \"additionalProperties\": false\n"
      + "}";

  private static final JsonSchema recordSchema = new JsonSchema(recordSchemaString);

  private static final String arraySchemaString =
      "{\"type\": \"array\", \"items\": { \"type\": \"string\" } }";

  private static final JsonSchema arraySchema = new JsonSchema(arraySchemaString);

  private static final String unionSchemaString =
      "{\n"
      + "  \"oneOf\": [\n"
      + "    { \"type\": \"string\", \"maxLength\": 5 },\n"
      + "    { \"type\": \"number\", \"minimum\": 0 }\n"
      + "  ]\n"
      + "}";

  private static final JsonSchema unionSchema = new JsonSchema(unionSchemaString);

  private static final String enumSchemaString =
      "{ \"type\": \"string\", \"enum\": [\"red\", \"amber\", \"green\"] }";

  private static final JsonSchema enumSchema = new JsonSchema(enumSchemaString);

  @Test
  public void testPrimitiveTypesToJsonSchema() {
    Object envelope = new JsonSchemaConverter().toObject(null, createPrimitiveSchema("null"));
    JsonNode result = (JsonNode) JsonSchemaUtils.getValue(envelope);
    assertEquals(NullNode.getInstance(), result);

    envelope = new JsonSchemaConverter().toObject(TestUtils.jsonTree("true"), createPrimitiveSchema("boolean"));
    result = (JsonNode) JsonSchemaUtils.getValue(envelope);
    assertEquals(true, ((BooleanNode)result).asBoolean());

    envelope = new JsonSchemaConverter().toObject(TestUtils.jsonTree("false"), createPrimitiveSchema("boolean"));
    result = (JsonNode) JsonSchemaUtils.getValue(envelope);
    assertEquals(false, ((BooleanNode)result).asBoolean());

    envelope = new JsonSchemaConverter().toObject(TestUtils.jsonTree("12"), createPrimitiveSchema("number"));
    result = (JsonNode) JsonSchemaUtils.getValue(envelope);
    assertEquals(12, ((NumericNode)result).asInt());

    envelope = new JsonSchemaConverter().toObject(TestUtils.jsonTree("23.2"), createPrimitiveSchema("number"));
    result = (JsonNode) JsonSchemaUtils.getValue(envelope);
    assertEquals(23.2, ((NumericNode)result).asDouble(), 0.1);

    envelope = new JsonSchemaConverter().toObject(TestUtils.jsonTree("\"a string\""), createPrimitiveSchema("string"));
    result = (JsonNode) JsonSchemaUtils.getValue(envelope);
    assertEquals("a string", ((TextNode)result).asText());
  }

  @Test
  public void testRecordToJsonSchema() {
    String json = "{\n"
            + "    \"null\": null,\n"
            + "    \"boolean\": true,\n"
            + "    \"number\": 12,\n"
            + "    \"string\": \"string\"\n"
            + "}";

    JsonNode envelope = (JsonNode) new JsonSchemaConverter().toObject(TestUtils.jsonTree(json), recordSchema);
    JsonNode result = (JsonNode) JsonSchemaUtils.getValue(envelope);
    assertEquals(true, result.get("null").isNull());
    assertEquals(true, result.get("boolean").booleanValue());
    assertEquals(12, result.get("number").intValue());
    assertEquals("string", result.get("string").textValue());
  }

  @Test(expected = ConversionException.class)
  public void testInvalidRecordToJsonSchema() {
    String json = "{\n"
                  + "    \"null\": null,\n"
                  + "    \"boolean\": true,\n"
                  + "    \"number\": 12,\n"
                  + "    \"string\": \"string\",\n"
                  + "    \"badString\": \"string\"\n"
                  + "}";

    JsonNode envelope = (JsonNode) new JsonSchemaConverter().toObject(TestUtils.jsonTree(json), recordSchema);
    JsonNode result = (JsonNode) JsonSchemaUtils.getValue(envelope);
    assertEquals(true, result.get("null").isNull());
    assertEquals(true, result.get("boolean").booleanValue());
    assertEquals(12, result.get("number").intValue());
    assertEquals("string", result.get("string").textValue());
  }

  @Test
  public void testArrayToJsonSchema() {
    String json = "[\"one\", \"two\", \"three\"]";

    Object envelope = new JsonSchemaConverter().toObject(TestUtils.jsonTree(json), arraySchema);
    JsonNode result = (JsonNode) JsonSchemaUtils.getValue(envelope);
    ArrayNode arrayNode = (ArrayNode) result;
    Iterator<JsonNode> elements = arrayNode.elements();
    List<String> strings = new ArrayList<String>();
    while (elements.hasNext()) {
      strings.add(elements.next().textValue());
    }
    assertArrayEquals(new String[]{"one", "two", "three"}, strings.toArray());
  }

  @Test
  public void testUnionToJsonSchema() {
    Object envelope = new JsonSchemaConverter().toObject(TestUtils.jsonTree("\"test\""), unionSchema);
    JsonNode result = (JsonNode) JsonSchemaUtils.getValue(envelope);
    assertEquals("test", ((TextNode)result).asText());

    envelope = new JsonSchemaConverter().toObject(TestUtils.jsonTree("12"), unionSchema);
    result = (JsonNode) JsonSchemaUtils.getValue(envelope);
    assertEquals(12, ((NumericNode)result).asInt());

    try {
      new JsonSchemaConverter().toObject(TestUtils.jsonTree("-1"), unionSchema);
      fail("Trying to use negative number should fail");
    } catch (ConversionException e) {
      // expected
    }
  }

  @Test
  public void testEnumToJsonSchema() {
    Object envelope = new JsonSchemaConverter().toObject(TestUtils.jsonTree("\"red\""), enumSchema);
    JsonNode result = (JsonNode) JsonSchemaUtils.getValue(envelope);
    assertEquals("red", ((TextNode)result).asText());

    try {
      new JsonSchemaConverter().toObject(TestUtils.jsonTree("\"yellow\""), enumSchema);
      fail("Trying to use non-enum should fail");
    } catch (ConversionException e) {
      // expected
    }
  }

  @Test
  public void testPrimitiveTypesToJson() {
    JsonSchemaConverter.JsonNodeAndSize result = new JsonSchemaConverter().toJson((int) 0);
    assertTrue(result.getJson().isNumber());
    assertTrue(result.getSize() > 0);

    result = new JsonSchemaConverter().toJson((long) 0);
    assertTrue(result.getJson().isNumber());

    result = new JsonSchemaConverter().toJson(0.1f);
    assertTrue(result.getJson().isNumber());

    result = new JsonSchemaConverter().toJson(0.1);
    assertTrue(result.getJson().isNumber());

    result = new JsonSchemaConverter().toJson(true);
    assertTrue(result.getJson().isBoolean());

    // "Primitive" here refers to JsonSchema primitive types, which are returned as standalone objects,
    // which can't have attached schemas. This includes, for example, Strings and byte[] even
    // though they are not Java primitives

    result = new JsonSchemaConverter().toJson("abcdefg");
    assertTrue(result.getJson().isTextual());
    assertEquals("abcdefg", result.getJson().textValue());
  }

  @Test
  public void testRecordToJson() throws Exception {
    String json = "{\n"
            + "    \"null\": null,\n"
            + "    \"boolean\": true,\n"
            + "    \"number\": 12,\n"
            + "    \"string\": \"string\"\n"
            + "}";
    JsonNode data = new ObjectMapper().readTree(json);
    JsonSchemaConverter.JsonNodeAndSize result = new JsonSchemaConverter().toJson(data);
    assertTrue(result.getSize() > 0);
    assertTrue(result.getJson().isObject());
    assertTrue(result.getJson().get("null").isNull());
    assertTrue(result.getJson().get("boolean").isBoolean());
    assertEquals(true, result.getJson().get("boolean").booleanValue());
    assertTrue(result.getJson().get("number").isIntegralNumber());
    assertEquals(12, result.getJson().get("number").intValue());
    assertTrue(result.getJson().get("string").isTextual());
    assertEquals("string", result.getJson().get("string").textValue());
  }

  @Test
  public void testArrayToJson() throws Exception {
    String json = "[\"one\", \"two\", \"three\"]";
    JsonNode data = new ObjectMapper().readTree(json);
    JsonSchemaConverter.JsonNodeAndSize result = new JsonSchemaConverter().toJson(data);
    assertTrue(result.getSize() > 0);

    assertTrue(result.getJson().isArray());
    assertEquals(3, result.getJson().size());
    assertEquals(JsonNodeFactory.instance.textNode("one"), result.getJson().get(0));
    assertEquals(JsonNodeFactory.instance.textNode("two"), result.getJson().get(1));
    assertEquals(JsonNodeFactory.instance.textNode("three"), result.getJson().get(2));
  }

  private static JsonSchema createPrimitiveSchema(String type) {
    String schemaString = String.format("{\"type\" : \"%s\"}", type);
    return new JsonSchema(schemaString);
  }

}
