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

import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import javax.ws.rs.client.Entity;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.Response;

import io.confluent.kafkarest.Errors;
import io.confluent.kafkarest.Versions;
import io.confluent.kafkarest.entities.BinaryConsumerRecord;
import io.confluent.kafkarest.entities.ConsumerInstanceConfig;
import io.confluent.kafkarest.entities.EmbeddedFormat;
import io.confluent.kafkarest.entities.Partition;
import io.confluent.kafkarest.entities.PartitionReplica;
import io.confluent.kafkarest.entities.Topic;
import io.confluent.rest.exceptions.ConstraintViolationExceptionMapper;
import kafka.utils.TestUtils;
import scala.collection.JavaConversions;

import static io.confluent.kafkarest.TestUtils.assertErrorResponse;

public class ConsumerBinaryTest extends AbstractConsumerTest {

  private static final String topicName = "topic1";
  private static final List<Partition> partitions = Arrays.asList(
      new Partition(0, 0, Arrays.asList(
          new PartitionReplica(0, true, true),
          new PartitionReplica(1, false, false)
      ))
  );
  private static final Topic topic = new Topic(topicName, new Properties(), partitions);
  private static final String groupName = "testconsumergroup";

  private final List<ProducerRecord<byte[], byte[]>> recordsOnlyValues = Arrays.asList(
      new ProducerRecord<byte[], byte[]>(topicName, "value".getBytes()),
      new ProducerRecord<byte[], byte[]>(topicName, "value2".getBytes()),
      new ProducerRecord<byte[], byte[]>(topicName, "value3".getBytes()),
      new ProducerRecord<byte[], byte[]>(topicName, "value4".getBytes())
  );

  private final List<ProducerRecord<byte[], byte[]>> recordsWithKeys = Arrays.asList(
      new ProducerRecord<byte[], byte[]>(topicName, "key".getBytes(), "value".getBytes()),
      new ProducerRecord<byte[], byte[]>(topicName, "key".getBytes(), "value2".getBytes()),
      new ProducerRecord<byte[], byte[]>(topicName, "key".getBytes(), "value3".getBytes()),
      new ProducerRecord<byte[], byte[]>(topicName, "key".getBytes(), "value4".getBytes())
  );

  private static final GenericType<List<BinaryConsumerRecord>> binaryConsumerRecordType
      = new GenericType<List<BinaryConsumerRecord>>() {
  };

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    final int numPartitions = 3;
    final int replicationFactor = 1;
    TestUtils.createTopic(zkClient, topicName, numPartitions, replicationFactor,
                          JavaConversions.asScalaBuffer(this.servers), new Properties());
  }

  @Test
  public void testConsumeOnlyValues() {
    // Between these tests we either leave the config null or request the binary embedded format
    // so we can test that both will result in binary consumers. We also us varying accept
    // parameters to test that we default to Binary for various values.
    String instanceUri = startConsumeMessages(groupName, topicName, null,
                                              Versions.KAFKA_V1_JSON_BINARY);
    produceBinaryMessages(recordsOnlyValues);
    consumeMessages(instanceUri, topicName, recordsOnlyValues,
                    Versions.KAFKA_V1_JSON_BINARY, Versions.KAFKA_V1_JSON_BINARY,
                    binaryConsumerRecordType, null);
    commitOffsets(instanceUri);
  }

  @Test
  public void testConsumeWithKeys() {
    String instanceUri = startConsumeMessages(groupName, topicName, EmbeddedFormat.BINARY,
                                              Versions.KAFKA_V1_JSON_BINARY);
    produceBinaryMessages(recordsWithKeys);
    consumeMessages(instanceUri, topicName, recordsWithKeys,
                    Versions.KAFKA_V1_JSON_BINARY, Versions.KAFKA_V1_JSON_BINARY,
                    binaryConsumerRecordType, null);
    commitOffsets(instanceUri);
  }

  @Test
  public void testConsumeWithAcceptAllHeader() {
    // This test ensures that Accept: */* defaults to binary
    String instanceUri = startConsumeMessages(groupName, topicName, EmbeddedFormat.BINARY,
        Versions.KAFKA_V1_JSON_BINARY);
    produceBinaryMessages(recordsWithKeys);
    consumeMessages(instanceUri, topicName, recordsWithKeys,
        Versions.ANYTHING, Versions.KAFKA_V1_JSON_BINARY,
        binaryConsumerRecordType, null);
    commitOffsets(instanceUri);
  }

  @Test
  public void testConsumeInvalidTopic() {
    startConsumeMessages(groupName, "nonexistenttopic", null,
                         Versions.KAFKA_V1_JSON_BINARY, true);
  }

  @Test
  public void testConsumeTimeout() {
    String instanceUri = startConsumeMessages(groupName, topicName, EmbeddedFormat.BINARY,
                                              Versions.KAFKA_V1_JSON_BINARY);
    produceBinaryMessages(recordsWithKeys);
    consumeMessages(instanceUri, topicName, recordsWithKeys,
                    Versions.KAFKA_V1_JSON_BINARY, Versions.KAFKA_V1_JSON_BINARY,
                    binaryConsumerRecordType, null);
    consumeForTimeout(
        instanceUri, topicName,
        Versions.KAFKA_V1_JSON_BINARY, Versions.KAFKA_V1_JSON_BINARY, binaryConsumerRecordType
    );
  }

  @Test
  public void testDeleteConsumer() {
    String instanceUri = startConsumeMessages(groupName, topicName, null,
                                              Versions.KAFKA_V1_JSON_BINARY);
    produceBinaryMessages(recordsWithKeys);
    consumeMessages(instanceUri, topicName, recordsWithKeys,
                    Versions.KAFKA_V1_JSON_BINARY, Versions.KAFKA_V1_JSON_BINARY,
                    binaryConsumerRecordType, null);
    deleteConsumer(instanceUri);
  }


  // The following tests are only included in the binary consumer because they test functionality
  // that isn't specific to the type of embedded data, but since they need
  @Test
  public void testInvalidKafkaConsumerConfig() {
    ConsumerInstanceConfig config = new ConsumerInstanceConfig("id", "name", "binary",
                                                               "bad-config", null);
    Response response = request("/consumers/" + groupName)
        .post(Entity.entity(config, Versions.KAFKA_V1_JSON));
    assertErrorResponse(ConstraintViolationExceptionMapper.UNPROCESSABLE_ENTITY, response,
                        Errors.INVALID_CONSUMER_CONFIG_ERROR_CODE,
                        Errors.INVALID_CONSUMER_CONFIG_MESSAGE,
                        Versions.KAFKA_V1_JSON);
  }


  @Test
  public void testDuplicateConsumerID() {
    String instanceUrl = startConsumeMessages(groupName, topicName, null,
                                              Versions.KAFKA_V1_JSON_BINARY);
    produceBinaryMessages(recordsWithKeys);

    // Duplicate the same instance, which should cause a conflict
    String name = consumerNameFromInstanceUrl(instanceUrl);
    Response createResponse = createConsumerInstance(groupName, null, name, null);
    assertErrorResponse(Response.Status.CONFLICT, createResponse,
                        Errors.CONSUMER_ALREADY_EXISTS_ERROR_CODE,
                        Errors.CONSUMER_ALREADY_EXISTS_MESSAGE,
                        Versions.KAFKA_MOST_SPECIFIC_DEFAULT);
  }
}
