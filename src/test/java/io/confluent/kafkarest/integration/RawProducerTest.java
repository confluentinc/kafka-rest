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

import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafkarest.Versions;
import io.confluent.kafkarest.entities.*;
import kafka.serializer.Decoder;
import org.apache.kafka.common.errors.SerializationException;
import org.junit.Before;
import org.junit.Test;
import scala.collection.JavaConversions;

import java.util.*;

public class RawProducerTest extends AbstractProducerTest {

  private String topicName = "topic1";
  private KafkaRawDecoder decoder = new KafkaRawDecoder();

  public class KafkaRawDecoder implements Decoder<String> {


    @Override
    public String fromBytes(byte[] bytes) {
      try {
        return new String(bytes);
      } catch (Exception e) {
        throw new SerializationException(e);
      }
    }
  }

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    final int numPartitions = 3;
    final int replicationFactor = 1;
    kafka.utils.TestUtils.createTopic(zkUtils, topicName, numPartitions, replicationFactor,
        JavaConversions.asScalaBuffer(this.servers),
        new Properties());
  }

  @Override
  protected String getEmbeddedContentType() {
    return Versions.KAFKA_V1_JSON_RAW;
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

  private final List<RawTopicProduceRecord> topicRecordsWithKeys = Arrays.asList(
      new RawTopicProduceRecord("key", "value", 0),
      new RawTopicProduceRecord("key", null, 0),
      new RawTopicProduceRecord("key", 53.4, 0),
      new RawTopicProduceRecord("key", 45, 0),
      new RawTopicProduceRecord("key", exampleMapValue(), 0),
      new RawTopicProduceRecord("key", exampleListValue(), 0)
  );

  private final List<RawTopicProduceRecord> topicRecordsWithoutKeys = Arrays.asList(
      new RawTopicProduceRecord("value", 0),
      new RawTopicProduceRecord(null, 0),
      new RawTopicProduceRecord(53.4, 0),
      new RawTopicProduceRecord(45, 0),
      new RawTopicProduceRecord(exampleMapValue(), 0),
      new RawTopicProduceRecord(exampleListValue(), 0)
  );

  private final List<RawProduceRecord> partitionRecordsWithKeys = Arrays.asList(
      new RawProduceRecord("key", "value"),
      new RawProduceRecord("key", null),
      new RawProduceRecord("key", 53.4),
      new RawProduceRecord("key", 45),
      new RawProduceRecord("key", exampleMapValue()),
      new RawProduceRecord("key", exampleListValue())
  );

  private final List<RawProduceRecord> partitionRecordsWithoutKeys = Arrays.asList(
      new RawProduceRecord("value"),
      new RawProduceRecord(null),
      new RawProduceRecord(53.4),
      new RawProduceRecord(45),
      new RawProduceRecord(exampleMapValue()),
      new RawProduceRecord(exampleListValue())
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
    testProduceToTopic(topicName, topicRecordsWithKeys, decoder, decoder,
        produceOffsets, true);
  }

  @Test
  public void testProduceToTopicNoKey() {
    testProduceToTopic(topicName, topicRecordsWithoutKeys, decoder, decoder,
        produceOffsets, true);
  }

  @Test
  public void testProduceToPartitionKeyAndValue() {
    testProduceToPartition(topicName, 0, partitionRecordsWithKeys, decoder, decoder,
        produceOffsets);
  }

  @Test
  public void testProduceToPartitionNoKey() {
    testProduceToPartition(topicName, 0, partitionRecordsWithoutKeys, decoder, decoder,
        produceOffsets);
  }

}
