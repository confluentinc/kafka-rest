/*
 * Copyright 2023 Confluent Inc.
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
import static org.junit.jupiter.api.Assertions.assertNotNull;

import io.confluent.rest.entities.ErrorMessage;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import org.apache.kafka.common.errors.SerializationException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class KafkaRestExceptionMapperTest {

  private KafkaRestExceptionMapper exceptionMapper;

  @BeforeEach
  public void setUp() {
    exceptionMapper = new KafkaRestExceptionMapper(null);
  }

  @Test
  public void test_whenSerializationExceptionThrown_thenCorrectResponseStatusIsSet() {
    verifyMapperResponse(
        new SerializationException("some message"), Status.REQUEST_TIMEOUT, 40801, "some message");
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
