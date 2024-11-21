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

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.confluent.kafkarest.Versions;
import io.confluent.kafkarest.entities.v2.BinaryPartitionProduceRequest;
import io.confluent.kafkarest.entities.v2.BinaryPartitionProduceRequest.BinaryPartitionProduceRecord;
import io.confluent.kafkarest.entities.v2.BinaryTopicProduceRequest;
import io.confluent.kafkarest.entities.v2.BinaryTopicProduceRequest.BinaryTopicProduceRecord;
import io.confluent.kafkarest.entities.v2.PartitionOffset;
import java.util.Arrays;
import java.util.List;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Response;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ProducerTest
    extends AbstractProducerTest<BinaryTopicProduceRequest, BinaryPartitionProduceRequest> {

  private static final String topicName = "topic1";

  // Produce to topic inputs & results

  private final List<BinaryTopicProduceRecord> topicRecordsWithKeys =
      Arrays.asList(
          new BinaryTopicProduceRecord("key", "value", null),
          new BinaryTopicProduceRecord("key", "value2", null),
          new BinaryTopicProduceRecord("key", "value3", null),
          new BinaryTopicProduceRecord("key", "value4", null));

  private final List<BinaryTopicProduceRecord> topicRecordsWithPartitions =
      Arrays.asList(
          new BinaryTopicProduceRecord(null, "value", 0),
          new BinaryTopicProduceRecord(null, "value2", 1),
          new BinaryTopicProduceRecord(null, "value3", 1),
          new BinaryTopicProduceRecord(null, "value4", 2));

  private final List<BinaryTopicProduceRecord> topicRecordsWithPartitionsAndKeys =
      Arrays.asList(
          new BinaryTopicProduceRecord("key", "value", 0),
          new BinaryTopicProduceRecord("key2", "value2", 1),
          new BinaryTopicProduceRecord("key3", "value3", 1),
          new BinaryTopicProduceRecord("key4", "value4", 2));

  private final List<BinaryTopicProduceRecord> topicRecordsWithNullValues =
      Arrays.asList(
          new BinaryTopicProduceRecord("key", null, null),
          new BinaryTopicProduceRecord("key", null, null),
          new BinaryTopicProduceRecord("key", null, null),
          new BinaryTopicProduceRecord("key", null, null));

  // Produce to partition inputs & results
  private final List<BinaryPartitionProduceRecord> partitionRecordsOnlyValues =
      Arrays.asList(
          new BinaryPartitionProduceRecord(null, "value"),
          new BinaryPartitionProduceRecord(null, "value2"),
          new BinaryPartitionProduceRecord(null, "value3"),
          new BinaryPartitionProduceRecord(null, "value4"));

  private final List<BinaryPartitionProduceRecord> partitionRecordsWithKeys =
      Arrays.asList(
          new BinaryPartitionProduceRecord("key", "value"),
          new BinaryPartitionProduceRecord("key", "value2"),
          new BinaryPartitionProduceRecord("key", "value3"),
          new BinaryPartitionProduceRecord("key", "value4"));

  private final List<BinaryPartitionProduceRecord> partitionRecordsWithNullValues =
      Arrays.asList(
          new BinaryPartitionProduceRecord("key1", null),
          new BinaryPartitionProduceRecord("key2", null),
          new BinaryPartitionProduceRecord("key3", null),
          new BinaryPartitionProduceRecord("key4", null));

  private final List<PartitionOffset> produceOffsets =
      Arrays.asList(
          new PartitionOffset(0, 0L, null, null),
          new PartitionOffset(0, 1L, null, null),
          new PartitionOffset(0, 2L, null, null),
          new PartitionOffset(0, 3L, null, null));

  private final List<PartitionOffset> producePartitionedOffsets =
      Arrays.asList(
          new PartitionOffset(0, 0L, null, null),
          new PartitionOffset(1, 0L, null, null),
          new PartitionOffset(1, 1L, null, null),
          new PartitionOffset(2, 0L, null, null));

  @BeforeEach
  @Override
  public void setUp() throws Exception {
    super.setUp();
    final int numPartitions = 3;
    final short replicationFactor = 1;
    createTopic(topicName, numPartitions, replicationFactor);
  }

  @Test
  public void testProduceToTopicWithKeys() {
    BinaryTopicProduceRequest request = BinaryTopicProduceRequest.create(topicRecordsWithKeys);
    testProduceToTopic(
        topicName,
        request,
        ByteArrayDeserializer.class.getName(),
        ByteArrayDeserializer.class.getName(),
        produceOffsets,
        false,
        request.toProduceRequest().getRecords());
  }

  @Test
  public void testProduceToTopicWithPartitions() {
    BinaryTopicProduceRequest request =
        BinaryTopicProduceRequest.create(topicRecordsWithPartitions);
    testProduceToTopic(
        topicName,
        request,
        ByteArrayDeserializer.class.getName(),
        ByteArrayDeserializer.class.getName(),
        producePartitionedOffsets,
        true,
        request.toProduceRequest().getRecords());
  }

  @Test
  public void testProduceToTopicWithPartitionsAndKeys() {
    BinaryTopicProduceRequest request =
        BinaryTopicProduceRequest.create(topicRecordsWithPartitionsAndKeys);
    testProduceToTopic(
        topicName,
        request,
        ByteArrayDeserializer.class.getName(),
        ByteArrayDeserializer.class.getName(),
        producePartitionedOffsets,
        true,
        request.toProduceRequest().getRecords());
  }

  @Test
  public void testProduceToTopicWithNullValues() {
    BinaryTopicProduceRequest request =
        BinaryTopicProduceRequest.create(topicRecordsWithNullValues);
    testProduceToTopic(
        topicName,
        request,
        ByteArrayDeserializer.class.getName(),
        ByteArrayDeserializer.class.getName(),
        produceOffsets,
        false,
        request.toProduceRequest().getRecords());
  }

  @Test
  public void testProduceToInvalidTopic() {
    BinaryTopicProduceRequest request = BinaryTopicProduceRequest.create(topicRecordsWithKeys);
    // This test turns auto-create off, so this should generate an error. Ideally it would
    // generate a 404, but Kafka as of 0.8.2.0 doesn't generate the correct exception, see
    // KAFKA-1884. For now this will just show up as failure to send all the messages.
    testProduceToTopicFails("invalid-topic", request);
  }

  protected void testProduceToPartition(
      List<BinaryPartitionProduceRecord> records, List<PartitionOffset> offsetResponse) {
    BinaryPartitionProduceRequest request = BinaryPartitionProduceRequest.create(records);
    testProduceToPartition(
        topicName,
        0,
        request,
        ByteArrayDeserializer.class.getName(),
        ByteArrayDeserializer.class.getName(),
        offsetResponse,
        request.toProduceRequest().getRecords());
  }

  @Test
  public void testProduceToPartitionOnlyValues() {
    testProduceToPartition(partitionRecordsOnlyValues, produceOffsets);
  }

  @Test
  public void testProduceToPartitionWithKeys() {
    testProduceToPartition(partitionRecordsWithKeys, produceOffsets);
  }

  @Test
  public void testProduceToPartitionWithNullValues() {
    testProduceToPartition(partitionRecordsWithNullValues, produceOffsets);
  }

  @Test
  public void testNullPayload() {

    List<String> versions =
        Arrays.asList(
            Versions.KAFKA_V2_JSON_AVRO,
            Versions.KAFKA_V2_JSON_JSON,
            Versions.KAFKA_V2_JSON_BINARY);

    for (String version : versions) {
      Response response = request("/topics/" + topicName).post(Entity.entity(null, version));
      assertEquals(422, response.getStatus(), "Produces to topic failed using " + version);
    }

    for (String version : versions) {
      Response response =
          request("/topics/" + topicName + " /partitions/0").post(Entity.entity(null, version));
      assertEquals(
          422, response.getStatus(), "Produces to topic partition failed using " + version);
    }
  }
}
