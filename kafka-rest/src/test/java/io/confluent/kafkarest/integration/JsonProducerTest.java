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

import io.confluent.kafka.serializers.KafkaJsonDeserializer;
import io.confluent.kafkarest.Versions;
import io.confluent.kafkarest.entities.v2.JsonPartitionProduceRequest;
import io.confluent.kafkarest.entities.v2.JsonPartitionProduceRequest.JsonPartitionProduceRecord;
import io.confluent.kafkarest.entities.v2.JsonTopicProduceRequest;
import io.confluent.kafkarest.entities.v2.JsonTopicProduceRequest.JsonTopicProduceRecord;
import io.confluent.kafkarest.entities.v2.PartitionOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.junit.Before;
import org.junit.Test;
import scala.collection.JavaConverters;

public class JsonProducerTest
    extends AbstractProducerTest<JsonTopicProduceRequest, JsonPartitionProduceRequest> {

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

  private Map<String, Object> exampleMapValue() {
    Map<String, Object> res = new HashMap<String, Object>();
    res.put("foo", "bar");
    res.put("bar", null);
    res.put("baz", 53.4);
    res.put("taz", 45);
    return res;
  }

  private List<Object> exampleListValue() {
    List<Object> res = new ArrayList<Object>();
    res.add("foo");
    res.add(null);
    res.add(53.4);
    res.add(45);
    res.add(exampleMapValue());
    return res;
  }

  private final List<JsonTopicProduceRecord> topicRecordsWithKeys = Arrays.asList(
      new JsonTopicProduceRecord("key", "value", 0),
      new JsonTopicProduceRecord("key", null, 0),
      new JsonTopicProduceRecord("key", 53.4, 0),
      new JsonTopicProduceRecord("key", 45, 0),
      new JsonTopicProduceRecord("key", exampleMapValue(), 0),
      new JsonTopicProduceRecord("key", exampleListValue(), 0)
  );

  private final List<JsonTopicProduceRecord> topicRecordsWithoutKeys = Arrays.asList(
      new JsonTopicProduceRecord(null, "value", 0),
      new JsonTopicProduceRecord(null, null, 0),
      new JsonTopicProduceRecord(null, 53.4, 0),
      new JsonTopicProduceRecord(null, 45, 0),
      new JsonTopicProduceRecord(null, exampleMapValue(), 0),
      new JsonTopicProduceRecord(null, exampleListValue(), 0)
  );

  private final List<JsonPartitionProduceRecord> partitionRecordsWithKeys = Arrays.asList(
      new JsonPartitionProduceRecord("key", "value"),
      new JsonPartitionProduceRecord("key", null),
      new JsonPartitionProduceRecord("key", 53.4),
      new JsonPartitionProduceRecord("key", 45),
      new JsonPartitionProduceRecord("key", exampleMapValue()),
      new JsonPartitionProduceRecord("key", exampleListValue())
  );

  private final List<JsonPartitionProduceRecord> partitionRecordsWithoutKeys = Arrays.asList(
      new JsonPartitionProduceRecord(null, "value"),
      new JsonPartitionProduceRecord(null, null),
      new JsonPartitionProduceRecord(null, 53.4),
      new JsonPartitionProduceRecord(null, 45),
      new JsonPartitionProduceRecord(null, exampleMapValue()),
      new JsonPartitionProduceRecord(null, exampleListValue())
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
    JsonTopicProduceRequest request = JsonTopicProduceRequest.create(topicRecordsWithKeys);
    testProduceToTopic(
        topicName,
        request,
        KafkaJsonDeserializer.class.getName(),
        KafkaJsonDeserializer.class.getName(),
        produceOffsets,
        true,
        request.toProduceRequest().getRecords());
  }

  @Test
  public void testProduceToTopicNoKey() {
    JsonTopicProduceRequest request = JsonTopicProduceRequest.create(topicRecordsWithoutKeys);
    testProduceToTopic(
        topicName,
        request,
        KafkaJsonDeserializer.class.getName(),
        KafkaJsonDeserializer.class.getName(),
        produceOffsets,
        true,
        request.toProduceRequest().getRecords());
  }

  @Test
  public void testProduceToPartitionKeyAndValue() {
    JsonPartitionProduceRequest request =
        JsonPartitionProduceRequest.create(partitionRecordsWithKeys);
    testProduceToPartition(
        topicName,
        0,
        request,
        KafkaJsonDeserializer.class.getName(),
        KafkaJsonDeserializer.class.getName(),
        produceOffsets,
        request.toProduceRequest().getRecords());
  }

  @Test
  public void testProduceToPartitionNoKey() {
    JsonPartitionProduceRequest request =
        JsonPartitionProduceRequest.create(partitionRecordsWithoutKeys);
    testProduceToPartition(
        topicName,
        0,
        request,
        KafkaJsonDeserializer.class.getName(),
        KafkaJsonDeserializer.class.getName(),
        produceOffsets,
        request.toProduceRequest().getRecords());
  }

}
