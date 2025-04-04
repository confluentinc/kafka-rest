/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.kafkarest.integration;

import static io.confluent.kafkarest.TestUtils.TEST_WITH_PARAMETERIZED_QUORUM_NAME;
import static io.confluent.kafkarest.TestUtils.assertErrorResponse;

import io.confluent.kafkarest.Errors;
import io.confluent.kafkarest.Versions;
import io.confluent.kafkarest.entities.EmbeddedFormat;
import io.confluent.kafkarest.entities.v2.BinaryConsumerRecord;
import io.confluent.kafkarest.entities.v2.CreateConsumerInstanceRequest;
import io.confluent.rest.exceptions.ConstraintViolationExceptionMapper;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.core.GenericType;
import jakarta.ws.rs.core.Response;
import java.util.Arrays;
import java.util.List;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class ConsumerBinaryTest extends AbstractConsumerTest {

  private static final String topicName = "topic1";
  private static final String groupName = "testconsumergroup";

  private final List<ProducerRecord<byte[], byte[]>> recordsOnlyValues =
      Arrays.asList(
          new ProducerRecord<>(topicName, "value".getBytes()),
          new ProducerRecord<>(topicName, "value2".getBytes()),
          new ProducerRecord<>(topicName, "value3".getBytes()),
          new ProducerRecord<>(topicName, "value4".getBytes()));

  private final List<ProducerRecord<byte[], byte[]>> recordsWithKeys =
      Arrays.asList(
          new ProducerRecord<>(topicName, "key".getBytes(), "value".getBytes()),
          new ProducerRecord<>(topicName, "key".getBytes(), "value2".getBytes()),
          new ProducerRecord<>(topicName, "key".getBytes(), "value3".getBytes()),
          new ProducerRecord<>(topicName, "key".getBytes(), "value4".getBytes()));

  private static final GenericType<List<BinaryConsumerRecord>> binaryConsumerRecordType =
      new GenericType<List<BinaryConsumerRecord>>() {};

  @BeforeEach
  @Override
  public void setUp(TestInfo testInfo) throws Exception {
    super.setUp(testInfo);
    final int numPartitions = 3;
    final int replicationFactor = 1;
    createTopic(topicName, numPartitions, (short) replicationFactor);
  }

  @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
  @ValueSource(strings = {"kraft"})
  public void testConsumeOnlyValues(String quorum) {
    // Between these tests we either leave the config null or request the binary embedded format
    // so we can test that both will result in binary consumers. We also us varying accept
    // parameters to test that we default to Binary for various values.
    String instanceUri =
        startConsumeMessages(groupName, topicName, null, Versions.KAFKA_V2_JSON_BINARY, "earliest");
    produceBinaryMessages(recordsOnlyValues);
    consumeMessages(
        instanceUri,
        recordsOnlyValues,
        Versions.KAFKA_V2_JSON_BINARY,
        Versions.KAFKA_V2_JSON_BINARY,
        binaryConsumerRecordType,
        /* converter= */ null,
        BinaryConsumerRecord::toConsumerRecord);
    commitOffsets(instanceUri);
  }

  @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
  @ValueSource(strings = {"kraft"})
  public void testConsumeWithKeys(String quorum) {
    String instanceUri =
        startConsumeMessages(
            groupName, topicName, EmbeddedFormat.BINARY, Versions.KAFKA_V2_JSON_BINARY, "earliest");
    produceBinaryMessages(recordsWithKeys);
    consumeMessages(
        instanceUri,
        recordsWithKeys,
        Versions.KAFKA_V2_JSON_BINARY,
        Versions.KAFKA_V2_JSON_BINARY,
        binaryConsumerRecordType,
        /* converter= */ null,
        BinaryConsumerRecord::toConsumerRecord);
    commitOffsets(instanceUri);
  }

  @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
  @ValueSource(strings = {"kraft"})
  public void testConsumeWithAcceptAllHeader(String quorum) {
    // This test ensures that Accept: */* defaults to binary
    String instanceUri =
        startConsumeMessages(
            groupName, topicName, EmbeddedFormat.BINARY, Versions.KAFKA_V2_JSON_BINARY, "earliest");
    produceBinaryMessages(recordsWithKeys);
    consumeMessages(
        instanceUri,
        recordsWithKeys,
        Versions.ANYTHING,
        Versions.KAFKA_V2_JSON_BINARY,
        binaryConsumerRecordType,
        /* converter= */ null,
        BinaryConsumerRecord::toConsumerRecord);
    commitOffsets(instanceUri);
  }

  @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
  @ValueSource(strings = {"kraft"})
  public void testConsumeTimeout(String quorum) {
    String instanceUri =
        startConsumeMessages(
            groupName, topicName, EmbeddedFormat.BINARY, Versions.KAFKA_V2_JSON_BINARY, "earliest");
    produceBinaryMessages(recordsWithKeys);
    consumeMessages(
        instanceUri,
        recordsWithKeys,
        Versions.KAFKA_V2_JSON_BINARY,
        Versions.KAFKA_V2_JSON_BINARY,
        binaryConsumerRecordType,
        /* converter= */ null,
        BinaryConsumerRecord::toConsumerRecord);
    consumeForTimeout(
        instanceUri,
        Versions.KAFKA_V2_JSON_BINARY,
        Versions.KAFKA_V2_JSON_BINARY,
        binaryConsumerRecordType);
  }

  @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
  @ValueSource(strings = {"kraft"})
  public void testDeleteConsumer(String quorum) {
    String instanceUri =
        startConsumeMessages(groupName, topicName, null, Versions.KAFKA_V2_JSON_BINARY, "earliest");
    produceBinaryMessages(recordsWithKeys);
    consumeMessages(
        instanceUri,
        recordsWithKeys,
        Versions.KAFKA_V2_JSON_BINARY,
        Versions.KAFKA_V2_JSON_BINARY,
        binaryConsumerRecordType,
        /* converter= */ null,
        BinaryConsumerRecord::toConsumerRecord);
    deleteConsumer(instanceUri);
  }

  // The following tests are only included in the binary consumer because they test functionality
  // that isn't specific to the type of embedded data, but since they need
  @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
  @ValueSource(strings = {"kraft"})
  public void testInvalidKafkaConsumerConfig(String quorum) {
    CreateConsumerInstanceRequest config =
        new CreateConsumerInstanceRequest(
            /* id= */ "id",
            /* name= */ "name",
            /* format= */ "binary",
            /* autoOffsetReset= */ "bad-config",
            /* autoCommitEnable */ null,
            /* responseMinBytes= */ null,
            /* requestWaitMs= */ null);
    Response response =
        request("/consumers/" + groupName).post(Entity.entity(config, Versions.KAFKA_V2_JSON));
    assertErrorResponse(
        ConstraintViolationExceptionMapper.UNPROCESSABLE_ENTITY,
        response,
        Errors.INVALID_CONSUMER_CONFIG_ERROR_CODE,
        Errors.INVALID_CONSUMER_CONFIG_MESSAGE,
        Versions.KAFKA_V2_JSON);
  }

  @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
  @ValueSource(strings = {"kraft"})
  public void testDuplicateConsumerID(String quorum) {
    String instanceUrl =
        startConsumeMessages(groupName, topicName, null, Versions.KAFKA_V2_JSON_BINARY, "earliest");
    produceBinaryMessages(recordsWithKeys);

    // Duplicate the same instance, which should cause a conflict
    String name = consumerNameFromInstanceUrl(instanceUrl);
    Response createResponse = createConsumerInstance(groupName, null, name, null, "earliest");
    assertErrorResponse(
        Response.Status.CONFLICT,
        createResponse,
        Errors.CONSUMER_ALREADY_EXISTS_ERROR_CODE,
        Errors.CONSUMER_ALREADY_EXISTS_MESSAGE,
        Versions.KAFKA_V2_JSON);
  }
}
