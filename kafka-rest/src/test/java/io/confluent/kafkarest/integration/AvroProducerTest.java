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

import com.fasterxml.jackson.databind.JsonNode;

import org.apache.avro.Schema;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Response;

import io.confluent.kafka.serializers.KafkaAvroDecoder;
import io.confluent.kafkarest.TestUtils;
import io.confluent.kafkarest.Versions;
import io.confluent.kafkarest.entities.AvroProduceRecord;
import io.confluent.kafkarest.entities.AvroTopicProduceRecord;
import io.confluent.kafkarest.entities.Partition;
import io.confluent.kafkarest.entities.PartitionOffset;
import io.confluent.kafkarest.entities.PartitionProduceRequest;
import io.confluent.kafkarest.entities.PartitionReplica;
import io.confluent.kafkarest.entities.ProduceRecord;
import io.confluent.kafkarest.entities.ProduceResponse;
import io.confluent.kafkarest.entities.TopicProduceRecord;
import io.confluent.kafkarest.entities.TopicProduceRequest;
import kafka.utils.VerifiableProperties;
import scala.collection.JavaConversions;

import static io.confluent.kafkarest.TestUtils.assertOKResponse;
import static org.junit.Assert.assertEquals;

// This test is much lighter than the Binary one which exercises all variants. Since binary
// covers most code paths well, this just tries to exercise Avro-specific parts.
public class AvroProducerTest extends ClusterTestHarness {

  private static final String topicName = "topic1";
  private static final List<Partition> partitions = Arrays.asList(
      new Partition(0, 0, Arrays.asList(
          new PartitionReplica(0, true, true),
          new PartitionReplica(1, false, false)
      ))
  );

  private KafkaAvroDecoder avroDecoder;

  // This test assumes that AvroConverterTest is good enough and testing one primitive type for
  // keys and one complex type for records is sufficient.
  private static final String keySchemaStr = "{\"name\":\"int\",\"type\": \"int\"}";
  private static final String valueSchemaStr = "{\"type\": \"record\", "
                                               + "\"name\":\"test\","
                                               + "\"fields\":[{"
                                               + "  \"name\":\"field\", "
                                               + "  \"type\": \"int\""
                                               + "}]}";
  private static final Schema valueSchema = new Schema.Parser().parse(valueSchemaStr);

  private final static JsonNode[] testKeys = {
      TestUtils.jsonTree("1"),
      TestUtils.jsonTree("2"),
      TestUtils.jsonTree("3"),
      TestUtils.jsonTree("4")
  };

  private final static JsonNode[] testValues = {
      TestUtils.jsonTree("{\"field\": 1}"),
      TestUtils.jsonTree("{\"field\": 2}"),
      TestUtils.jsonTree("{\"field\": 3}"),
      TestUtils.jsonTree("{\"field\": 4}"),
  };

  // Produce to topic inputs & results

  private final List<AvroTopicProduceRecord> topicRecordsWithPartitionsAndKeys = Arrays.asList(
      new AvroTopicProduceRecord(testKeys[0], testValues[0], 0),
      new AvroTopicProduceRecord(testKeys[1], testValues[1], 1),
      new AvroTopicProduceRecord(testKeys[2], testValues[2], 1),
      new AvroTopicProduceRecord(testKeys[3], testValues[3], 2)
  );
  private final List<PartitionOffset> partitionOffsetsWithPartitionsAndKeys = Arrays.asList(
      new PartitionOffset(0, 0L, null, null),
      new PartitionOffset(0, 1L, null, null),
      new PartitionOffset(1, 0L, null, null),
      new PartitionOffset(1, 1L, null, null)
  );

  // Produce to partition inputs & results
  private final List<AvroProduceRecord> partitionRecordsOnlyValues = Arrays.asList(
      new AvroProduceRecord(testValues[0]),
      new AvroProduceRecord(testValues[1]),
      new AvroProduceRecord(testValues[2]),
      new AvroProduceRecord(testValues[3])
  );
  private final List<PartitionOffset> producePartitionOffsetOnlyValues = Arrays.asList(
      new PartitionOffset(0, 0L, null, null),
      new PartitionOffset(0, 1L, null, null),
      new PartitionOffset(0, 2L, null, null),
      new PartitionOffset(0, 3L, null, null)
  );

  public AvroProducerTest() {
    super(1, true);
  }

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    final int numPartitions = 3;
    final int replicationFactor = 1;
    kafka.utils.TestUtils.createTopic(zkClient, topicName, numPartitions, replicationFactor,
                                      JavaConversions.asScalaBuffer(this.servers),
                                      new Properties());

    Properties props = new Properties();
    props.setProperty("schema.registry.url", schemaRegConnect);
    avroDecoder = new KafkaAvroDecoder(new VerifiableProperties(props));
  }

  private <K, V> void testProduceToTopic(List<? extends TopicProduceRecord> records,
                                         List<PartitionOffset> offsetResponses) {
    TopicProduceRequest payload = new TopicProduceRequest();
    payload.setRecords(records);
    payload.setKeySchema(keySchemaStr);
    payload.setValueSchema(valueSchemaStr);
    Response response = request("/topics/" + topicName)
        .post(Entity.entity(payload, Versions.KAFKA_V1_JSON_AVRO));
    assertOKResponse(response, Versions.KAFKA_MOST_SPECIFIC_DEFAULT);
    final ProduceResponse produceResponse = TestUtils.tryReadEntityOrLog(response, ProduceResponse.class);
    TestUtils.assertPartitionOffsetsEqual(offsetResponses, produceResponse.getOffsets());
    TestUtils.assertTopicContains(zkConnect, topicName,
                                  payload.getRecords(), null,
                                  avroDecoder, avroDecoder, false);
    assertEquals(produceResponse.getKeySchemaId(), (Integer) 1);
    assertEquals(produceResponse.getValueSchemaId(), (Integer) 2);
  }


  @Test
  public void testProduceToTopicWithPartitionsAndKeys() {
    testProduceToTopic(topicRecordsWithPartitionsAndKeys, partitionOffsetsWithPartitionsAndKeys);
  }

  private <K, V> void testProduceToPartition(List<? extends ProduceRecord<K, V>> records,
                                             List<PartitionOffset> offsetResponse) {
    PartitionProduceRequest payload = new PartitionProduceRequest();
    payload.setRecords(records);
    payload.setValueSchema(valueSchemaStr);
    Response response = request("/topics/" + topicName + "/partitions/0")
        .post(Entity.entity(payload, Versions.KAFKA_V1_JSON_AVRO));
    assertOKResponse(response, Versions.KAFKA_MOST_SPECIFIC_DEFAULT);
    final ProduceResponse poffsetResponse
        = TestUtils.tryReadEntityOrLog(response, ProduceResponse.class);
    assertEquals(offsetResponse, poffsetResponse.getOffsets());
    TestUtils.assertTopicContains(zkConnect, topicName,
                                  payload.getRecords(), (Integer) 0,
                                  avroDecoder, avroDecoder, false);
    assertEquals((Integer) 1, poffsetResponse.getValueSchemaId());
  }

  @Test
  public void testProduceToPartitionOnlyValues() {
    testProduceToPartition(partitionRecordsOnlyValues, producePartitionOffsetOnlyValues);
  }
}
