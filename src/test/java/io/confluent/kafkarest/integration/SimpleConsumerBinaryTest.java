/**
 * Copyright 2015 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/
package io.confluent.kafkarest.integration;

import io.confluent.kafkarest.Errors;
import io.confluent.kafkarest.Versions;
import io.confluent.kafkarest.entities.*;
import jersey.repackaged.com.google.common.collect.ImmutableMap;
import kafka.utils.TestUtils;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Before;
import org.junit.Test;
import scala.collection.JavaConversions;

import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.Response;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static io.confluent.kafkarest.TestUtils.assertErrorResponse;
import static io.confluent.kafkarest.TestUtils.assertOKResponse;
import static org.junit.Assert.assertEquals;

public class SimpleConsumerBinaryTest extends AbstractConsumerTest {

  private static final String topicName = "topic1";

  private final List<ProducerRecord<byte[], byte[]>> recordsOnlyValues = Arrays.asList(
      new ProducerRecord<byte[], byte[]>(topicName, "value".getBytes()),
      new ProducerRecord<byte[], byte[]>(topicName, "value2".getBytes()),
      new ProducerRecord<byte[], byte[]>(topicName, "value3".getBytes()),
      new ProducerRecord<byte[], byte[]>(topicName, "value4".getBytes())
  );

  private static final GenericType<List<BinaryConsumerRecord>> binaryConsumerRecordType
      = new GenericType<List<BinaryConsumerRecord>>() {
  };

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    final int numPartitions = 1;
    final int replicationFactor = 1;
    TestUtils.createTopic(zkClient, topicName, numPartitions, replicationFactor,
                          JavaConversions.asScalaIterable(this.servers).toSeq(), new Properties());
  }

  @Test
  public void testConsumeOnlyOffset() {

    // Load Kafka with some messages
    produceBinaryMessages(recordsOnlyValues);

    // No count param specified => we expect only one message
    Response response = request("/topics/" + topicName + "/partition/0/messages",
        ImmutableMap.of("offset", "0")).accept(Versions.KAFKA_V1_JSON_BINARY).get();
    assertOKResponse(response, Versions.KAFKA_V1_JSON_BINARY);
    List<BinaryConsumerRecord> consumed = response.readEntity(binaryConsumerRecordType);
    assertEquals(1, consumed.size());

    // The response record is compared only with the first input record
    assertEqualsMessages(recordsOnlyValues.subList(0, 1), consumed, null);
  }

  @Test
  public void testConsumeOffsetAndCount() {
    // Load Kafka with some messages
    produceBinaryMessages(recordsOnlyValues);

    // We want to retrieve all input records
    Response response = request("/topics/" + topicName + "/partition/0/messages",
        ImmutableMap.of("offset", "0", "count", Integer.toString(recordsOnlyValues.size())))
        .accept(Versions.KAFKA_V1_JSON_BINARY).get();
    assertOKResponse(response, Versions.KAFKA_V1_JSON_BINARY);
    List<BinaryConsumerRecord> consumed = response.readEntity(binaryConsumerRecordType);
    assertEquals(recordsOnlyValues.size(), consumed.size());

    // The response record is compared only with the first input record
    assertEqualsMessages(recordsOnlyValues, consumed, null);
  }

  @Test
  public void testConsumeInvalidTopic() {

    Response response = request("/topics/nonexistenttopic/partition/0/messages",
        ImmutableMap.of("offset", "0")).accept(Versions.KAFKA_V1_JSON_BINARY).get();

    assertErrorResponse(Response.Status.NOT_FOUND, response,
        Errors.TOPIC_NOT_FOUND_ERROR_CODE,
        Errors.TOPIC_NOT_FOUND_MESSAGE,
        Versions.KAFKA_V1_JSON_BINARY);
  }

  @Test
  public void testConsumeInvalidPartition() {

    Response response = request("/topics/topic1/partition/1/messages",
        ImmutableMap.of("offset", "0")).accept(Versions.KAFKA_V1_JSON_BINARY).get();

    assertErrorResponse(Response.Status.NOT_FOUND, response,
        Errors.PARTITION_NOT_FOUND_ERROR_CODE,
        Errors.PARTITION_NOT_FOUND_MESSAGE,
        Versions.KAFKA_V1_JSON_BINARY);
  }

}
