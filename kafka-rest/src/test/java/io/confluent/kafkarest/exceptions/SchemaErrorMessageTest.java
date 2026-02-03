/*
 * Copyright 2026 Confluent Inc.
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

package io.confluent.kafkarest.exceptions;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

public class SchemaErrorMessageTest {

  private final ObjectMapper mapper = new ObjectMapper();

  @Test
  @DisplayName("Constructor sets all fields correctly")
  public void testConstructorAndGetters() {
    SchemaErrorMessage message = new SchemaErrorMessage(40002, "Schema validation failed", 40901);

    assertEquals(40002, message.getErrorCode());
    assertEquals("Schema validation failed", message.getMessage());
    assertEquals(Integer.valueOf(40901), message.getSchemaErrorCode());
  }

  @Test
  @DisplayName("Constructor accepts null schemaErrorCode")
  public void testConstructorWithNullSchemaErrorCode() {
    SchemaErrorMessage message = new SchemaErrorMessage(40002, "Some error", null);

    assertEquals(40002, message.getErrorCode());
    assertEquals("Some error", message.getMessage());
    assertNull(message.getSchemaErrorCode());
  }

  @Test
  @DisplayName("JSON serialization includes schema_error_code when present")
  public void testSerializationWithSchemaErrorCode() throws Exception {
    SchemaErrorMessage message = new SchemaErrorMessage(40002, "Schema validation failed", 40901);

    String json = mapper.writeValueAsString(message);

    assertTrue(json.contains("\"error_code\":40002"));
    assertTrue(json.contains("\"message\":\"Schema validation failed\""));
    assertTrue(json.contains("\"schema_error_code\":40901"));
  }

  @Test
  @DisplayName("schema_error_code is omitted from JSON when null")
  public void testSerializationWithoutSchemaErrorCode() throws Exception {
    SchemaErrorMessage message = new SchemaErrorMessage(40002, "Some error", null);

    String json = mapper.writeValueAsString(message);

    assertTrue(json.contains("\"error_code\":40002"));
    assertTrue(json.contains("\"message\":\"Some error\""));
    assertFalse(json.contains("schema_error_code"));
  }

  @Test
  @DisplayName("equals() compares all fields including schemaErrorCode")
  public void testEquals() {
    SchemaErrorMessage msg1 = new SchemaErrorMessage(40002, "error", 40901);
    SchemaErrorMessage msg2 = new SchemaErrorMessage(40002, "error", 40901);
    SchemaErrorMessage msg3 = new SchemaErrorMessage(40002, "error", 40902);
    SchemaErrorMessage msg4 = new SchemaErrorMessage(40002, "error", null);

    assertEquals(msg1, msg2);
    assertNotEquals(msg1, msg3);
    assertNotEquals(msg1, msg4);
  }

  @Test
  @DisplayName("hashCode() includes schemaErrorCode")
  public void testHashCode() {
    SchemaErrorMessage msg1 = new SchemaErrorMessage(40002, "error", 40901);
    SchemaErrorMessage msg2 = new SchemaErrorMessage(40002, "error", 40901);
    SchemaErrorMessage msg3 = new SchemaErrorMessage(40002, "error", 40902);

    assertEquals(msg1.hashCode(), msg2.hashCode());
    assertNotEquals(msg1.hashCode(), msg3.hashCode());
  }
}
