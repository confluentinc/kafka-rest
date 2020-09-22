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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.DoubleNode;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import io.confluent.kafka.serializers.KafkaJsonDeserializer;
import io.confluent.kafkarest.Versions;
import io.confluent.kafkarest.entities.v2.PartitionOffset;
import io.confluent.kafkarest.entities.v2.ProduceRequest;
import io.confluent.kafkarest.entities.v2.ProduceRequest.ProduceRecord;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.junit.Before;
import org.junit.Test;
import scala.collection.JavaConverters;

public class JsonProducerTest extends AbstractProducerTest {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private String topicName = "topic1";

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

  @Override
  protected String getEmbeddedContentType() {
    return Versions.KAFKA_V2_JSON_JSON;
  }

  private ObjectNode exampleMapValue() {
    Map<String, JsonNode> res = new HashMap<>();
    res.put("foo", TextNode.valueOf("bar"));
    res.put("bar", NullNode.getInstance());
    res.put("baz", DoubleNode.valueOf(53.4));
    res.put("taz", IntNode.valueOf(45));
    return new ObjectNode(JsonNodeFactory.instance, res);
  }

  private ArrayNode exampleListValue() {
    List<JsonNode> res = new ArrayList<>();
    res.add(TextNode.valueOf("foo"));
    res.add(NullNode.getInstance());
    res.add(DoubleNode.valueOf(53.4));
    res.add(IntNode.valueOf(45));
    res.add(exampleMapValue());
    return new ArrayNode(JsonNodeFactory.instance, res);
  }

  private final List<ProduceRecord> topicRecordsWithKeys =
      Arrays.asList(
          ProduceRecord.create(/* partition= */ 0, TextNode.valueOf("key"), TextNode.valueOf("value")),
          ProduceRecord.create(/* partition= */ 0, TextNode.valueOf("key"), null),
          ProduceRecord.create(/* partition= */ 0, TextNode.valueOf("key"), DoubleNode.valueOf(53.4)),
          ProduceRecord.create(/* partition= */ 0, TextNode.valueOf("key"), IntNode.valueOf(45)),
          ProduceRecord.create(/* partition= */ 0, TextNode.valueOf("key"), exampleMapValue()),
          ProduceRecord.create(/* partition= */ 0, TextNode.valueOf("key"), exampleListValue()));

  private final List<ProduceRecord> topicRecordsWithoutKeys = Arrays.asList(
      ProduceRecord.create(/* partition= */ 0, /* key= */ null, TextNode.valueOf("value")),
      ProduceRecord.create(/* partition= */ 0, /* key= */ null, /* value= */ null),
      ProduceRecord.create(/* partition= */ 0, /* key= */ null, DoubleNode.valueOf(53.4)),
      ProduceRecord.create(/* partition= */ 0, /* key= */ null, IntNode.valueOf(45)),
      ProduceRecord.create(/* partition= */ 0, /* key= */ null, exampleMapValue()),
      ProduceRecord.create(/* partition= */ 0, /* key= */ null, exampleListValue())
  );

  private final List<ProduceRecord> partitionRecordsWithKeys = Arrays.asList(
      ProduceRecord.create(TextNode.valueOf("key"), TextNode.valueOf("value")),
      ProduceRecord.create(TextNode.valueOf("key"), /* value= */ null),
      ProduceRecord.create(TextNode.valueOf("key"), DoubleNode.valueOf(53.4)),
      ProduceRecord.create(TextNode.valueOf("key"), IntNode.valueOf(45)),
      ProduceRecord.create(TextNode.valueOf("key"), exampleMapValue()),
      ProduceRecord.create(TextNode.valueOf("key"), exampleListValue())
  );

  private final List<ProduceRecord> partitionRecordsWithoutKeys = Arrays.asList(
      ProduceRecord.create(/* key= */ null, TextNode.valueOf("value")),
      ProduceRecord.create(/* key= */ null, /* value= */ null),
      ProduceRecord.create(/* key= */ null, DoubleNode.valueOf(53.4)),
      ProduceRecord.create(/* key= */ null, IntNode.valueOf(45)),
      ProduceRecord.create(/* key= */ null, exampleMapValue()),
      ProduceRecord.create(/* key= */ null, exampleListValue())
  );

  private final List<PartitionOffset> produceOffsets = Arrays.asList(
      new PartitionOffset(0, 0L, null, null),
      new PartitionOffset(0, 1L, null, null),
      new PartitionOffset(0, 2L, null, null),
      new PartitionOffset(0, 3L, null, null),
      new PartitionOffset(0, 4L, null, null),
      new PartitionOffset(0, 5L, null, null)
  );

  @Test
  public void testProduceToTopicKeyAndValue() {
    ProduceRequest request = ProduceRequest.create(topicRecordsWithKeys);
    testProduceToTopic(
        topicName,
        request,
        record -> record.getValue().map(JsonProducerTest::treeToValue).orElse(null),
        new KafkaJsonDeserializer<>(),
        new KafkaJsonDeserializer<>(),
        produceOffsets,
        true,
        request.getRecords());
  }

  @Test
  public void testProduceToTopicNoKey() {
    ProduceRequest request = ProduceRequest.create(topicRecordsWithoutKeys);
    testProduceToTopic(
        topicName,
        request,
        record -> record.getValue().map(JsonProducerTest::treeToValue).orElse(null),
        new KafkaJsonDeserializer<>(),
        new KafkaJsonDeserializer<>(),
        produceOffsets,
        true,
        request.getRecords());
  }

  @Test
  public void testProduceToPartitionKeyAndValue() {
    ProduceRequest request = ProduceRequest.create(partitionRecordsWithKeys);
    testProduceToPartition(
        topicName,
        0,
        request,
        record -> record.getValue().map(JsonProducerTest::treeToValue).orElse(null),
        new KafkaJsonDeserializer<>(),
        new KafkaJsonDeserializer<>(),
        produceOffsets,
        request.getRecords());
  }

  @Test
  public void testProduceToPartitionNoKey() {
    ProduceRequest request = ProduceRequest.create(partitionRecordsWithoutKeys);
    testProduceToPartition(
        topicName,
        0,
        request,
        record -> record.getValue().map(JsonProducerTest::treeToValue).orElse(null),
        new KafkaJsonDeserializer<>(),
        new KafkaJsonDeserializer<>(),
        produceOffsets,
        request.getRecords());
  }

  private static Object treeToValue(JsonNode node) {
    try {
      return OBJECT_MAPPER.treeToValue(node, Object.class);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }
}
