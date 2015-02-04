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

import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Response;

import io.confluent.kafkarest.Errors;
import io.confluent.kafkarest.TestUtils;
import io.confluent.kafkarest.Versions;
import io.confluent.kafkarest.entities.BinaryProduceRecord;
import io.confluent.kafkarest.entities.BinaryTopicProduceRecord;
import io.confluent.kafkarest.entities.Partition;
import io.confluent.kafkarest.entities.PartitionOffset;
import io.confluent.kafkarest.entities.PartitionProduceRequest;
import io.confluent.kafkarest.entities.PartitionProduceResponse;
import io.confluent.kafkarest.entities.PartitionReplica;
import io.confluent.kafkarest.entities.ProduceRecord;
import io.confluent.kafkarest.entities.TopicProduceRecord;
import io.confluent.kafkarest.entities.TopicProduceRequest;
import io.confluent.kafkarest.entities.TopicProduceResponse;
import kafka.serializer.Decoder;
import kafka.serializer.DefaultDecoder;
import scala.collection.JavaConversions;

import static io.confluent.kafkarest.TestUtils.assertErrorResponse;
import static io.confluent.kafkarest.TestUtils.assertOKResponse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ProducerTest extends ClusterTestHarness {

  private static final String topicName = "topic1";
  private static final List<Partition> partitions = Arrays.asList(
      new Partition(0, 0, Arrays.asList(
          new PartitionReplica(0, true, true),
          new PartitionReplica(1, false, false)
      ))
  );

  private static final Decoder<byte[]> binaryDecoder = new DefaultDecoder(null);

  // Produce to topic inputs & results

  private final List<BinaryTopicProduceRecord> topicRecordsWithKeys = Arrays.asList(
      new BinaryTopicProduceRecord("key".getBytes(), "value".getBytes()),
      new BinaryTopicProduceRecord("key".getBytes(), "value2".getBytes()),
      new BinaryTopicProduceRecord("key".getBytes(), "value3".getBytes()),
      new BinaryTopicProduceRecord("key".getBytes(), "value4".getBytes())
  );
  private final List<PartitionOffset> partitionOffsetsWithKeys = Arrays.asList(
      new PartitionOffset(1, 3)
  );

  private final List<BinaryTopicProduceRecord> topicRecordsWithPartitions = Arrays.asList(
      new BinaryTopicProduceRecord("value".getBytes(), 0),
      new BinaryTopicProduceRecord("value2".getBytes(), 1),
      new BinaryTopicProduceRecord("value3".getBytes(), 0),
      new BinaryTopicProduceRecord("value4".getBytes(), 2)
  );
  private final List<PartitionOffset> partitionOffsetsWithPartitions = Arrays.asList(
      new PartitionOffset(0, 1),
      new PartitionOffset(1, 0),
      new PartitionOffset(2, 0)
  );

  private final List<BinaryTopicProduceRecord> topicRecordsWithPartitionsAndKeys = Arrays.asList(
      new BinaryTopicProduceRecord("key".getBytes(), "value".getBytes(), 0),
      new BinaryTopicProduceRecord("key2".getBytes(), "value2".getBytes(), 1),
      new BinaryTopicProduceRecord("key3".getBytes(), "value3".getBytes(), 1),
      new BinaryTopicProduceRecord("key4".getBytes(), "value4".getBytes(), 2)
  );
  private final List<PartitionOffset> partitionOffsetsWithPartitionsAndKeys = Arrays.asList(
      new PartitionOffset(0, 0),
      new PartitionOffset(1, 1),
      new PartitionOffset(2, 0)
  );

  private final List<BinaryTopicProduceRecord> topicRecordsWithNullValues = Arrays.asList(
      new BinaryTopicProduceRecord("key".getBytes(), (byte[]) null),
      new BinaryTopicProduceRecord("key".getBytes(), (byte[]) null),
      new BinaryTopicProduceRecord("key".getBytes(), (byte[]) null),
      new BinaryTopicProduceRecord("key".getBytes(), (byte[]) null)
  );
  private final List<PartitionOffset> partitionOffsetsWithNullValues = Arrays.asList(
      new PartitionOffset(1, 3)
  );

  // Produce to partition inputs & results
  private final List<BinaryProduceRecord> partitionRecordsOnlyValues = Arrays.asList(
      new BinaryProduceRecord("value".getBytes()),
      new BinaryProduceRecord("value2".getBytes()),
      new BinaryProduceRecord("value3".getBytes()),
      new BinaryProduceRecord("value4".getBytes())
  );
  private final PartitionOffset producePartitionOffsetOnlyValues = new PartitionOffset(0, 3);

  private final List<BinaryProduceRecord> partitionRecordsWithKeys = Arrays.asList(
      new BinaryProduceRecord("key".getBytes(), "value".getBytes()),
      new BinaryProduceRecord("key".getBytes(), "value2".getBytes()),
      new BinaryProduceRecord("key".getBytes(), "value3".getBytes()),
      new BinaryProduceRecord("key".getBytes(), "value4".getBytes())
  );
  private final PartitionOffset producePartitionOffsetWithKeys = new PartitionOffset(0, 3);

  private final List<BinaryProduceRecord> partitionRecordsWithNullValues = Arrays.asList(
      new BinaryProduceRecord("key1".getBytes(), null),
      new BinaryProduceRecord("key2".getBytes(), null),
      new BinaryProduceRecord("key3".getBytes(), null),
      new BinaryProduceRecord("key4".getBytes(), null)
  );
  private final PartitionOffset producePartitionOffsetWithNullValues = new PartitionOffset(0, 3);


  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    final int numPartitions = 3;
    final int replicationFactor = 1;
    kafka.utils.TestUtils.createTopic(zkClient, topicName, numPartitions, replicationFactor,
                                      JavaConversions.asScalaIterable(this.servers).toSeq(),
                                      new Properties());
  }

  private <K, V> void testProduceToTopic(List<? extends TopicProduceRecord> records,
                                         List<PartitionOffset> offsetResponses) {
    TopicProduceRequest payload = new TopicProduceRequest();
    payload.setRecords(records);
    Response response = request("/topics/" + topicName)
        .post(Entity.entity(payload, Versions.KAFKA_MOST_SPECIFIC_DEFAULT));
    assertOKResponse(response, Versions.KAFKA_MOST_SPECIFIC_DEFAULT);
    final TopicProduceResponse produceResponse = response.readEntity(TopicProduceResponse.class);
    assertTrue(TestUtils.partitionOffsetsEqual(offsetResponses, produceResponse.getOffsets()));
    TestUtils.assertTopicContains(zkConnect, topicName,
                                  payload.getRecords(), null,
                                  binaryDecoder, binaryDecoder, true);
  }

  @Test
  public void testProduceToTopicWithKeys() {
    testProduceToTopic(topicRecordsWithKeys, partitionOffsetsWithKeys);
  }

  @Test
  public void testProduceToTopicWithPartitions() {
    testProduceToTopic(topicRecordsWithPartitions, partitionOffsetsWithPartitions);
  }

  @Test
  public void testProduceToTopicWithPartitionsAndKeys() {
    testProduceToTopic(topicRecordsWithPartitionsAndKeys, partitionOffsetsWithPartitionsAndKeys);
  }

  @Test
  public void testProduceToTopicWithNullValues() {
    testProduceToTopic(topicRecordsWithNullValues, partitionOffsetsWithNullValues);
  }

  @Test
  public void testProduceToInvalidTopic() {
    TopicProduceRequest payload = new TopicProduceRequest();
    payload.setRecords(Arrays.asList(
        new BinaryTopicProduceRecord("key".getBytes(), "value".getBytes())
    ));
    final Response response = request("/topics/topicdoesnotexist")
        .post(Entity.entity(payload, Versions.KAFKA_MOST_SPECIFIC_DEFAULT));
    assertErrorResponse(Response.Status.NOT_FOUND, response,
                        Errors.TOPIC_NOT_FOUND_ERROR_CODE, Errors.TOPIC_NOT_FOUND_MESSAGE,
                        Versions.KAFKA_MOST_SPECIFIC_DEFAULT);
  }


  private <K, V> void testProduceToPartition(List<? extends ProduceRecord<K, V>> records,
                                             PartitionOffset offsetResponse) {
    PartitionProduceRequest payload = new PartitionProduceRequest();
    payload.setRecords(records);
    Response response = request("/topics/" + topicName + "/partitions/0")
        .post(Entity.entity(payload, Versions.KAFKA_MOST_SPECIFIC_DEFAULT));
    assertOKResponse(response, Versions.KAFKA_MOST_SPECIFIC_DEFAULT);
    final PartitionProduceResponse poffsetResponse
        = response.readEntity(PartitionProduceResponse.class);
    assertEquals(offsetResponse, poffsetResponse.getPartitionOffset());
    TestUtils.assertTopicContains(zkConnect, topicName,
                                  payload.getRecords(), (Integer) 0,
                                  binaryDecoder, binaryDecoder, true);
  }

  @Test
  public void testProduceToPartitionOnlyValues() {
    testProduceToPartition(partitionRecordsOnlyValues, producePartitionOffsetOnlyValues);
  }

  @Test
  public void testProduceToPartitionWithKeys() {
    testProduceToPartition(partitionRecordsWithKeys, producePartitionOffsetWithKeys);
  }

  @Test
  public void testProduceToPartitionWithNullValues() {
    testProduceToPartition(partitionRecordsWithNullValues, producePartitionOffsetWithNullValues);
  }
}
