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

package io.confluent.kafkarest.resources.v3;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.concurrent.CompletionException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.junit.jupiter.api.Test;

public class ProduceMetricErrorCodesTest {

  @Test
  public void authorizationErrorIsClientError() {
    assertEquals(403, status(new TopicAuthorizationException("denied")));
  }

  @Test
  public void unknownTopicIsClientError() {
    assertEquals(404, status(new UnknownTopicOrPartitionException("missing")));
  }

  @Test
  public void retriableKafkaErrorIsServerError() {
    assertEquals(5, status(new TimeoutException("timeout")) / 100);
  }

  @Test
  public void unwrapsCompletionException() {
    assertEquals(403, status(new CompletionException(new TopicAuthorizationException("denied"))));
  }

  private static int status(Throwable error) {
    return ProduceMetricErrorCodes.httpStatusCodeFromError(error);
  }
}
