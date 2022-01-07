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

package io.confluent.kafkarest.controllers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.confluent.kafkarest.entities.EmbeddedFormat;
import io.confluent.rest.exceptions.RestConstraintViolationException;
import java.util.Optional;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class SchemaRecordSerializerTest {

  @Test
  public void errorWhenNoSchemaRegistryDefined() {
    boolean checkpoint = false;
    try {
      SchemaRecordSerializer schemaRecordSerializer = new SchemaRecordSerializerThrowing();
      schemaRecordSerializer.serialize(EmbeddedFormat.AVRO, "topic", Optional.empty(), null, true);
    } catch (RestConstraintViolationException rcve) {
      assertEquals(42207, rcve.getErrorCode());
      assertEquals(
          "Error serializing message. Schema Registry not defined, no Schema Registry client available to serialize message.",
          rcve.getMessage());
      checkpoint = true;
    }
    assertTrue(checkpoint);
  }
}
