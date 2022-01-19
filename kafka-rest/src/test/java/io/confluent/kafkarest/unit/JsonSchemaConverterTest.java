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
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import io.confluent.kafkarest.converters.JsonSchemaConverter;
import org.junit.jupiter.api.Test;

public class JsonSchemaConverterTest {

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

    // "Primitive" here refers to JsonSchema primitive types, which are returned as standalone
    // objects,
    // which can't have attached schemas. This includes, for example, Strings and byte[] even
    // though they are not Java primitives

    result = new JsonSchemaConverter().toJson("abcdefg");
    assertTrue(result.getJson().isTextual());
    assertEquals("abcdefg", result.getJson().textValue());
  }

  @Test
  public void testRecordToJson() throws Exception {
    String json =
        "{\n"
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
}
