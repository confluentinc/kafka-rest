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

package io.confluent.kafkarest.v2;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.common.base.Strings;
import io.confluent.kafkarest.ConsumerInstanceId;
import io.confluent.kafkarest.ConsumerRecordAndSize;
import io.confluent.kafkarest.KafkaRestConfig;
import io.confluent.kafkarest.entities.ConsumerInstanceConfig;
import io.confluent.kafkarest.entities.EmbeddedFormat;
import java.nio.charset.StandardCharsets;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.junit.jupiter.api.Test;

public class JsonKafkaConsumerStateTest {

  private final JsonKafkaConsumerState state =
      new JsonKafkaConsumerState(
          new KafkaRestConfig(),
          ConsumerInstanceConfig.create(EmbeddedFormat.JSON),
          new ConsumerInstanceId("group", "instance"),
          new MockConsumer<>(OffsetResetStrategy.EARLIEST, "group"));

  @Test
  public void createConsumerRecord_valueLargerThanDefaultJacksonStringLimit_doesNotThrow() {
    // Regression test for INC-13627: values >20_000_000 chars used to throw
    // StreamConstraintsException.
    int stringLength = 20_000_001;
    String largeJsonStringValue = "\"" + Strings.repeat("a", stringLength) + "\"";
    ConsumerRecord<byte[], byte[]> record =
        new ConsumerRecord<>(
            "topic",
            /* partition= */ 0,
            /* offset= */ 0,
            /* key= */ null,
            largeJsonStringValue.getBytes(StandardCharsets.UTF_8));

    ConsumerRecordAndSize<Object, Object> result =
        assertDoesNotThrow(() -> state.createConsumerRecord(record));

    assertEquals(stringLength, ((String) result.getRecord().getValue()).length());
  }
}
