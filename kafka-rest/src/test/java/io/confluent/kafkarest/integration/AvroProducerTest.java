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

import static io.confluent.kafkarest.TestUtils.assertOKResponse;
import static org.junit.Assert.assertEquals;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.NullNode;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafkarest.TestUtils;
import io.confluent.kafkarest.Versions;
import io.confluent.kafkarest.entities.v2.PartitionOffset;
import io.confluent.kafkarest.entities.v2.ProduceRequest;
import io.confluent.kafkarest.entities.v2.ProduceRequest.ProduceRecord;
import io.confluent.kafkarest.entities.v2.ProduceResponse;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Response;
import org.junit.Before;
import org.junit.Test;

// This test is much lighter than the Binary one which exercises all variants. Since binary
// covers most code paths well, this just tries to exercise Schema-specific parts.
public class AvroProducerTest extends ClusterTestHarness {

  private static final String topicName = "topic1";

  protected Properties deserializerProps;

  // This test assumes that SchemaConverterTest is good enough and testing one primitive type for
  // keys and one complex type for records is sufficient.
  private static final String keySchemaStr = "{\"name\":\"int\",\"type\": \"int\"}";
  private static final String valueSchemaStr = "{\"type\": \"record\", "
                                               + "\"name\":\"test\","
                                               + "\"fields\":[{"
                                               + "  \"name\":\"field\", "
                                               + "  \"type\": \"int\""
                                               + "}]}";
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

  protected final List<ProduceRecord> topicRecordsWithPartitionsAndKeys =
      Arrays.asList(
          ProduceRecord.create(/* partition= */ 0, testKeys[0], testValues[0]),
          ProduceRecord.create(/* partition= */ 1, testKeys[1], testValues[1]),
          ProduceRecord.create(/* partition= */ 1, testKeys[2], testValues[2]),
          ProduceRecord.create(/* partition= */ 2, testKeys[3], testValues[3]));
  protected final List<PartitionOffset> partitionOffsetsWithPartitionsAndKeys = Arrays.asList(
      new PartitionOffset(0, 0L, null, null),
      new PartitionOffset(0, 1L, null, null),
      new PartitionOffset(1, 0L, null, null),
      new PartitionOffset(1, 1L, null, null)
  );

  // Produce to partition inputs & results
  protected final List<ProduceRecord> partitionRecordsOnlyValues =
      Arrays.asList(
          ProduceRecord.create(/* key= */ null, testValues[0]),
          ProduceRecord.create(/* key= */ null, testValues[1]),
          ProduceRecord.create(/* key= */ null, testValues[2]),
          ProduceRecord.create(/* key= */ null, testValues[3]));
  protected final List<PartitionOffset> producePartitionOffsetOnlyValues = Arrays.asList(
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
    createTopic(topicName, /* numPartitions= */ 3, /* replicationFactor= */ (short) 1);

    deserializerProps = new Properties();
    deserializerProps.setProperty("schema.registry.url", schemaRegConnect);
  }

  protected void testProduceToTopic(
      List<ProduceRecord> records, List<PartitionOffset> offsetResponses
  ) {
    testProduceToTopic(records, offsetResponses, Collections.emptyMap());
  }

  protected void testProduceToTopic(
      List<ProduceRecord> records,
      List<PartitionOffset> offsetResponses,
      Map<String, String> queryParams
  ) {
    ProduceRequest payload =
        ProduceRequest.create(
            records,
            /* keySchemaId= */ null,
            keySchemaStr,
            /* valueSchemaId= */ null,
            valueSchemaStr);
    Response response = request("/topics/" + topicName, queryParams)
        .post(Entity.entity(payload, Versions.KAFKA_V2_JSON_AVRO));
    assertOKResponse(response, Versions.KAFKA_V2_JSON);
    final ProduceResponse produceResponse = TestUtils
        .tryReadEntityOrLog(response, ProduceResponse.class);
    TestUtils.assertPartitionOffsetsEqual(offsetResponses, produceResponse.getOffsets());
    TestUtils.assertTopicContains(
        plaintextBrokerList,
        topicName,
        null,
        payload.getRecords(),
        record ->
            record.getValue().orElse(NullNode.getInstance()),
        new KafkaAvroDeserializer(),
        new KafkaAvroDeserializer(),
        deserializerProps,
        false);
    assertEquals(produceResponse.getKeySchemaId(), (Integer) 1);
    assertEquals(produceResponse.getValueSchemaId(), (Integer) 2);
  }

  @Test
  public void testProduceToTopicWithPartitionsAndKeys() {
    testProduceToTopic(topicRecordsWithPartitionsAndKeys, partitionOffsetsWithPartitionsAndKeys);
  }

  protected void testProduceToPartition(
      List<ProduceRecord> records, List<PartitionOffset> offsetResponse
  ) {
    testProduceToPartition(records, offsetResponse, Collections.emptyMap());
  }

  protected void testProduceToPartition(List<ProduceRecord> records,
                                        List<PartitionOffset> offsetResponse,
                                        Map<String, String> queryParams) {
    ProduceRequest payload =
        ProduceRequest.create(
            records,
            /* keySchemaId= */ null,
            /* keySchema= */ null,
            /* valueSchemaId= */ null,
            valueSchemaStr);
    Response response = request("/topics/" + topicName + "/partitions/0", queryParams)
        .post(Entity.entity(payload, Versions.KAFKA_V2_JSON_AVRO));
    assertOKResponse(response, Versions.KAFKA_V2_JSON);
    final ProduceResponse poffsetResponse
        = TestUtils.tryReadEntityOrLog(response, ProduceResponse.class);
    assertEquals(offsetResponse, poffsetResponse.getOffsets());
    TestUtils.assertTopicContains(
        plaintextBrokerList,
        topicName,
        0,
        payload.getRecords(),
        record ->
            record.getValue().orElse(NullNode.getInstance()),
        new KafkaAvroDeserializer(),
        new KafkaAvroDeserializer(),
        deserializerProps,
        false);
    assertEquals((Integer) 1, poffsetResponse.getValueSchemaId());
  }

  @Test
  public void testProduceToPartitionOnlyValues() {
    testProduceToPartition(partitionRecordsOnlyValues, producePartitionOffsetOnlyValues);
  }
}
