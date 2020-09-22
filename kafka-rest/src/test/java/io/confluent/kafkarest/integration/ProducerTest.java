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

import static org.junit.Assert.assertEquals;

import com.fasterxml.jackson.databind.node.TextNode;
import com.google.common.io.BaseEncoding;
import com.google.protobuf.ByteString;
import io.confluent.kafkarest.KafkaRestConfig;
import io.confluent.kafkarest.ProducerPool;
import io.confluent.kafkarest.Versions;
import io.confluent.kafkarest.entities.v2.PartitionOffset;
import io.confluent.kafkarest.entities.v2.ProduceRequest;
import io.confluent.kafkarest.entities.v2.ProduceRequest.ProduceRecord;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Response;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.Before;
import org.junit.Test;
import scala.collection.JavaConverters;

public class ProducerTest extends AbstractProducerTest {

  private static final String topicName = "topic1";

  private static TextNode base64Encode(String value) {
    return TextNode.valueOf(BaseEncoding.base64().encode(value.getBytes(StandardCharsets.UTF_8)));
  }

  // Produce to topic inputs & results

  private final List<ProduceRecord> topicRecordsWithKeys =
      Arrays.asList(
          ProduceRecord.create(base64Encode("key"), base64Encode("value")),
          ProduceRecord.create(base64Encode("key"), base64Encode("value2")),
          ProduceRecord.create(base64Encode("key"), base64Encode("value3")),
          ProduceRecord.create(base64Encode("key"), base64Encode("value4")));

  private final List<ProduceRecord> topicRecordsWithPartitions =
      Arrays.asList(
          ProduceRecord.create(
              /* partition= */ 0, /* key= */ null, base64Encode("value")),
          ProduceRecord.create(
              /* partition= */ 1, /* key= */ null, base64Encode("value2")),
          ProduceRecord.create(
              /* partition= */ 1, /* key= */ null, base64Encode("value3")),
          ProduceRecord.create(
              /* partition= */ 2, /* key= */ null, base64Encode("value4")));

  private final List<ProduceRecord> topicRecordsWithPartitionsAndKeys =
      Arrays.asList(
          ProduceRecord.create(
              /* partition= */ 0, base64Encode("key"), base64Encode("value")),
          ProduceRecord.create(
              /* partition= */ 1, base64Encode("key"), base64Encode("value2")),
          ProduceRecord.create(
              /* partition= */ 1, base64Encode("key"), base64Encode("value3")),
          ProduceRecord.create(
              /* partition= */ 2, base64Encode("key"), base64Encode("value4")));

  private final List<ProduceRecord> topicRecordsWithNullValues =
      Arrays.asList(
          ProduceRecord.create(base64Encode("key"), /* value= */ null),
          ProduceRecord.create(base64Encode("key"), /* value= */ null),
          ProduceRecord.create(base64Encode("key"), /* value= */ null),
          ProduceRecord.create(base64Encode("key"), /* value= */ null));

  // Produce to partition inputs & results
  private final List<ProduceRecord> partitionRecordsOnlyValues =
      Arrays.asList(
          ProduceRecord.create(/* key= */ null, base64Encode("value")),
          ProduceRecord.create(/* key= */ null, base64Encode("value2")),
          ProduceRecord.create(/* key= */ null, base64Encode("value3")),
          ProduceRecord.create(/* key= */ null, base64Encode("value4")));

  private final List<ProduceRecord> partitionRecordsWithKeys =
      Arrays.asList(
          ProduceRecord.create(base64Encode("key"), base64Encode("value")),
          ProduceRecord.create(base64Encode("key"), base64Encode("value2")),
          ProduceRecord.create(base64Encode("key"), base64Encode("value3")),
          ProduceRecord.create(base64Encode("key"), base64Encode("value4")));

  private final List<ProduceRecord> partitionRecordsWithNullValues =
      Arrays.asList(
          ProduceRecord.create(base64Encode("key1"), /* value= */ null),
          ProduceRecord.create(base64Encode("key2"), /* value= */ null),
          ProduceRecord.create(base64Encode("key3"), /* value= */ null),
          ProduceRecord.create(base64Encode("key4"), /* value= */ null));

  private final List<PartitionOffset> produceOffsets = Arrays.asList(
      new PartitionOffset(0, 0L, null, null),
      new PartitionOffset(0, 1L, null, null),
      new PartitionOffset(0, 2L, null, null),
      new PartitionOffset(0, 3L, null, null)
  );

  private final List<PartitionOffset> producePartitionedOffsets = Arrays.asList(
      new PartitionOffset(0, 0L, null, null),
      new PartitionOffset(1, 0L, null, null),
      new PartitionOffset(1, 1L, null, null),
      new PartitionOffset(2, 0L, null, null)
  );

  @Override
  protected ProducerPool getProducerPool(KafkaRestConfig appConfig) {
    Properties overrides = new Properties();
    // Reduce the metadata fetch timeout so requests for topics that don't exist timeout much
    // faster than the default
    overrides.setProperty(ProducerConfig.MAX_BLOCK_MS_CONFIG, "5000");
    return new ProducerPool(appConfig, overrides);
  }

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    final int numPartitions = 3;
    final int replicationFactor = 1;
    kafka.utils.TestUtils.createTopic(zkClient, topicName, numPartitions, replicationFactor,
        JavaConverters.asScalaBuffer(this.servers),
        new Properties());
  }

  @Test
  public void testProduceToTopicWithKeys() {
    ProduceRequest request = ProduceRequest.create(topicRecordsWithKeys);
    testProduceToTopic(
        topicName,
        request,
        record ->
            record.getValue()
                .map(value -> ByteString.copyFrom(BaseEncoding.base64().decode(value.asText())))
                .orElse(null),
        (topic, data) -> ByteString.copyFrom(data),
        (topic, data) -> ByteString.copyFrom(data),
        produceOffsets,
        false,
        request.getRecords());
  }

  @Test
  public void testProduceToTopicWithPartitions() {
    ProduceRequest request = ProduceRequest.create(topicRecordsWithPartitions);
    testProduceToTopic(
        topicName,
        request,
        record ->
            record.getValue()
                .map(value -> ByteString.copyFrom(BaseEncoding.base64().decode(value.asText())))
                .orElse(null),
        (topic, data) -> ByteString.copyFrom(data),
        (topic, data) -> ByteString.copyFrom(data),
        producePartitionedOffsets,
        true,
        request.getRecords());
  }

  @Test
  public void testProduceToTopicWithPartitionsAndKeys() {
    ProduceRequest request = ProduceRequest.create(topicRecordsWithPartitionsAndKeys);
    testProduceToTopic(
        topicName,
        request,
        record ->
            record.getValue()
                .map(value -> ByteString.copyFrom(BaseEncoding.base64().decode(value.asText())))
                .orElse(null),
        (topic, data) -> ByteString.copyFrom(data),
        (topic, data) -> ByteString.copyFrom(data),
        producePartitionedOffsets,
        true,
        request.getRecords());
  }

  @Test
  public void testProduceToTopicWithNullValues() {
    ProduceRequest request = ProduceRequest.create(topicRecordsWithNullValues);
    testProduceToTopic(
        topicName,
        request,
        record ->
            record.getValue()
                .map(value -> ByteString.copyFrom(BaseEncoding.base64().decode(value.asText())))
                .orElse(null),
        (topic, data) -> ByteString.copyFrom(data),
        (topic, data) -> ByteString.copyFrom(data),
        produceOffsets,
        false,
        request.getRecords());
  }

  @Test
  public void testProduceToInvalidTopic() {
    ProduceRequest request = ProduceRequest.create(topicRecordsWithKeys);
    // This test turns auto-create off, so this should generate an error. Ideally it would
    // generate a 404, but Kafka as of 0.8.2.0 doesn't generate the correct exception, see
    // KAFKA-1884. For now this will just show up as failure to send all the messages.
    testProduceToTopicFails("invalid-topic", request);
  }


  protected void testProduceToPartition(List<ProduceRecord> records,
                                        List<PartitionOffset> offsetResponse) {
    ProduceRequest request = ProduceRequest.create(records);
    testProduceToPartition(
        topicName,
        0,
        request,
        record ->
            record.getValue()
                .map(value -> ByteString.copyFrom(BaseEncoding.base64().decode(value.asText())))
                .orElse(null),
        (topic, data) -> ByteString.copyFrom(data),
        (topic, data) -> ByteString.copyFrom(data),
        offsetResponse,
        request.getRecords());
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

    List<String> versions = Arrays.asList(
        Versions.KAFKA_V2_JSON_AVRO, Versions.KAFKA_V2_JSON_JSON, Versions.KAFKA_V2_JSON_BINARY
    );

    for (String version : versions) {
      Response response = request("/topics/" + topicName)
          .post(Entity.entity(null, version));
      assertEquals("Produces to topic failed using " + version, 422, response.getStatus());
    }

    for (String version : versions) {
      Response response = request("/topics/" + topicName + " /partitions/0")
          .post(Entity.entity(null, version));
      assertEquals("Produces to topic partition failed using " + version, 422,
          response.getStatus());
    }
  }
}
