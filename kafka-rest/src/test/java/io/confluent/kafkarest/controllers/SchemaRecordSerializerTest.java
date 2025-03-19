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
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.confluent.kafkarest.entities.EmbeddedFormat;
import io.confluent.rest.exceptions.RestConstraintViolationException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

public class SchemaRecordSerializerTest {

  @Test
  public void errorWhenNoSchemaRegistryDefined() {
    Map<String, Object> commonConfigs = new HashMap<>();
    RestConstraintViolationException rcve =
        assertThrows(
            RestConstraintViolationException.class,
            () ->
                new SchemaRecordSerializerImpl(
                        Optional.empty(), commonConfigs, commonConfigs, commonConfigs)
                    .serialize(EmbeddedFormat.AVRO, "topic", Optional.empty(), null, true));

    assertEquals(42207, rcve.getErrorCode());
    assertEquals(
        "Error serializing message. Schema Registry not defined, "
            + "no Schema Registry client available to serialize message.",
        rcve.getMessage());
  }
}
