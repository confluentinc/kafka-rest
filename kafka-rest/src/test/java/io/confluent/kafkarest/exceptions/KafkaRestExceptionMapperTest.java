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

import static io.confluent.rest.exceptions.KafkaExceptionMapper.KAFKA_BAD_REQUEST_ERROR_CODE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.confluent.rest.entities.ErrorMessage;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.Response.Status;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.common.errors.SerializationException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

public class KafkaRestExceptionMapperTest {

  private KafkaRestExceptionMapper exceptionMapper;

  @BeforeEach
  public void setUp() {
    exceptionMapper = new KafkaRestExceptionMapper(null);
  }

  @Test
  @DisplayName("SerializationException returns REQUEST_TIMEOUT with error code 40801")
  public void test_whenSerializationExceptionThrown_thenCorrectResponseStatusIsSet() {
    verifyMapperResponse(
        new SerializationException("some message"), Status.REQUEST_TIMEOUT, 40801, "some message");
  }

  @Test
  @DisplayName("InvalidConfigurationWithSchemaException returns BAD_REQUEST with schemaErrorCode")
  public void testSchemaValidationError() {
    InvalidConfigurationWithSchemaException exception =
        new InvalidConfigurationWithSchemaException("Schema validation failed", 40901);

    Response response = exceptionMapper.toResponse(exception);

    assertNotNull(response);
    assertEquals(Status.BAD_REQUEST.getStatusCode(), response.getStatus());
    assertTrue(response.getEntity() instanceof SchemaErrorMessage);
    SchemaErrorMessage errorMessage = (SchemaErrorMessage) response.getEntity();
    assertEquals(KAFKA_BAD_REQUEST_ERROR_CODE, errorMessage.getErrorCode());
    assertEquals("Schema validation failed", errorMessage.getMessage());
    assertEquals(Integer.valueOf(40901), errorMessage.getSchemaErrorCode());
  }

  @Test
  @DisplayName("schemaErrorCode 0 is converted to null in response")
  public void testSchemaValidationErrorWithZeroCode() {
    InvalidConfigurationWithSchemaException exception =
        new InvalidConfigurationWithSchemaException("Some error", 0);

    Response response = exceptionMapper.toResponse(exception);

    assertNotNull(response);
    assertEquals(Status.BAD_REQUEST.getStatusCode(), response.getStatus());
    SchemaErrorMessage errorMessage = (SchemaErrorMessage) response.getEntity();
    assertEquals(KAFKA_BAD_REQUEST_ERROR_CODE, errorMessage.getErrorCode());
    assertNull(errorMessage.getSchemaErrorCode());
  }

  @Test
  @DisplayName("InvalidConfigurationWithSchemaException wrapped in CompletionException is handled")
  public void testSchemaValidationErrorWrappedInCompletionException() {
    InvalidConfigurationWithSchemaException schemaException =
        new InvalidConfigurationWithSchemaException("Incompatible schema", 40901);
    CompletionException wrappedException = new CompletionException(schemaException);

    Response response = exceptionMapper.toResponse(wrappedException);

    assertNotNull(response);
    assertEquals(Status.BAD_REQUEST.getStatusCode(), response.getStatus());
    SchemaErrorMessage errorMessage = (SchemaErrorMessage) response.getEntity();
    assertEquals(KAFKA_BAD_REQUEST_ERROR_CODE, errorMessage.getErrorCode());
    assertEquals(Integer.valueOf(40901), errorMessage.getSchemaErrorCode());
  }

  @Test
  @DisplayName("InvalidConfigurationWithSchemaException wrapped in ExecutionException is handled")
  public void testSchemaValidationErrorWrappedInExecutionException() {
    InvalidConfigurationWithSchemaException schemaException =
        new InvalidConfigurationWithSchemaException("Association already exists", 40903);
    ExecutionException wrappedException = new ExecutionException(schemaException);

    Response response = exceptionMapper.toResponse(wrappedException);

    assertNotNull(response);
    assertEquals(Status.BAD_REQUEST.getStatusCode(), response.getStatus());
    SchemaErrorMessage errorMessage = (SchemaErrorMessage) response.getEntity();
    assertEquals(KAFKA_BAD_REQUEST_ERROR_CODE, errorMessage.getErrorCode());
    assertEquals(Integer.valueOf(40903), errorMessage.getSchemaErrorCode());
  }

  private void verifyMapperResponse(
      Throwable throwable, Status status, int errorCode, String exceptionMessage) {
    Response response = exceptionMapper.toResponse(throwable);
    assertNotNull(response);
    assertEquals(status.getStatusCode(), response.getStatus());
    ErrorMessage errorMessage = (ErrorMessage) response.getEntity();
    assertEquals(errorCode, errorMessage.getErrorCode());
    assertEquals(exceptionMessage, throwable.getMessage());
  }
}
