package io.confluent.kafkarest.controllers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.confluent.rest.exceptions.RestConstraintViolationException;
import java.util.Collections;
import java.util.Optional;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class SchemaRecordSerializerImplTest {

  @Test
  public void errorWhenNoSchemaRegistryDefined() {
    boolean checkpoint = false;
    try {
      SchemaRecordSerializerImpl schemaRecordSerializerImpl =
          new SchemaRecordSerializerImpl(
              Optional.empty(),
              Collections.EMPTY_MAP,
              Collections.EMPTY_MAP,
              Collections.EMPTY_MAP);
    } catch (RestConstraintViolationException rcve) {
      assertEquals(42207, rcve.getErrorCode());
      assertEquals(
          "Error deserializing message. Schema registry not defined, no Schema Registry client available to serialize message.",
          rcve.getMessage());
      checkpoint = true;
    }
    assertTrue(checkpoint);
  }
}
