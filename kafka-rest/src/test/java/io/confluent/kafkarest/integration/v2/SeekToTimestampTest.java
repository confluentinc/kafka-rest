/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.kafkarest.integration.v2;

import static io.confluent.kafkarest.TestUtils.TEST_WITH_PARAMETERIZED_QUORUM_NAME;

import io.confluent.kafkarest.Versions;
import io.confluent.kafkarest.entities.v2.BinaryConsumerRecord;
import io.confluent.kafkarest.integration.AbstractConsumerTest;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import javax.ws.rs.core.GenericType;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class SeekToTimestampTest extends AbstractConsumerTest {

  private static final String TOPIC_NAME = "topic-1";
  private static final String CONSUMER_GROUP_ID = "consumer-group-1";

  private static final List<ProducerRecord<byte[], byte[]>> RECORDS_BEFORE_TIMESTAMP =
      Arrays.asList(
          new ProducerRecord<>(TOPIC_NAME, "value-1".getBytes()),
          new ProducerRecord<>(TOPIC_NAME, "value-2".getBytes()),
          new ProducerRecord<>(TOPIC_NAME, "value-3".getBytes()));

  private static final List<ProducerRecord<byte[], byte[]>> RECORDS_AFTER_TIMESTAMP =
      Arrays.asList(
          new ProducerRecord<>(TOPIC_NAME, "value-4".getBytes()),
          new ProducerRecord<>(TOPIC_NAME, "value-5".getBytes()),
          new ProducerRecord<>(TOPIC_NAME, "value-6".getBytes()));

  private static final GenericType<List<BinaryConsumerRecord>> BINARY_CONSUMER_RECORD_TYPE =
      new GenericType<List<BinaryConsumerRecord>>() {};

  @BeforeEach
  @Override
  public void setUp(TestInfo testInfo) throws Exception {
    super.setUp(testInfo);
    createTopic(TOPIC_NAME, /* numPartitions= */ 1, /* replicationFactor= */ (short) 1);
  }

  @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
  @ValueSource(strings = {"kraft", "zk"})
  public void testConsumeOnlyValues(String quorum) throws Exception {
    String consumerUri =
        startConsumeMessages(
            CONSUMER_GROUP_ID,
            TOPIC_NAME,
            /* format= */ null,
            Versions.KAFKA_V2_JSON_BINARY,
            "latest");

    produceBinaryMessages(RECORDS_BEFORE_TIMESTAMP);

    Instant timestamp = Instant.now();
    Thread.sleep(1000);

    produceBinaryMessages(RECORDS_AFTER_TIMESTAMP);

    seekToTimestamp(consumerUri, TOPIC_NAME, /* partitionId= */ 0, timestamp);

    consumeMessages(
        consumerUri,
        RECORDS_AFTER_TIMESTAMP,
        Versions.KAFKA_V2_JSON_BINARY,
        Versions.KAFKA_V2_JSON_BINARY,
        BINARY_CONSUMER_RECORD_TYPE,
        /* converter= */ null,
        BinaryConsumerRecord::toConsumerRecord);

    commitOffsets(consumerUri);
  }
}
