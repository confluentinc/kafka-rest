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

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.security.JaasUtils;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Response;

import io.confluent.kafkarest.KafkaRestConfig;
import io.confluent.kafkarest.ProducerPool;
import io.confluent.kafkarest.RecordMetadataOrException;
import io.confluent.kafkarest.Versions;
import io.confluent.kafkarest.entities.BinaryProduceRecord;
import io.confluent.kafkarest.entities.BinaryTopicProduceRecord;
import io.confluent.kafkarest.entities.EmbeddedFormat;
import io.confluent.kafkarest.entities.PartitionOffset;
import io.confluent.kafkarest.entities.ProduceRecord;
import kafka.serializer.Decoder;
import kafka.serializer.DefaultDecoder;
import kafka.utils.ZkUtils;
import scala.collection.JavaConversions;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class ProducerTest extends AbstractProducerTest {

  private ZkUtils testZkUtils;

  private static final String topicName = "topic1";

  private static final Decoder<byte[]> binaryDecoder = new DefaultDecoder(null);

  // Produce to topic inputs & results

  private final List<BinaryTopicProduceRecord> topicRecordsWithKeys = Arrays.asList(
      new BinaryTopicProduceRecord("key".getBytes(), "value".getBytes()),
      new BinaryTopicProduceRecord("key".getBytes(), "value2".getBytes()),
      new BinaryTopicProduceRecord("key".getBytes(), "value3".getBytes()),
      new BinaryTopicProduceRecord("key".getBytes(), "value4".getBytes())
  );

  private final List<BinaryTopicProduceRecord> topicRecordsWithPartitions = Arrays.asList(
      new BinaryTopicProduceRecord("value".getBytes(), 0),
      new BinaryTopicProduceRecord("value2".getBytes(), 1),
      new BinaryTopicProduceRecord("value3".getBytes(), 1),
      new BinaryTopicProduceRecord("value4".getBytes(), 2)
  );

  private final List<BinaryTopicProduceRecord> topicRecordsWithPartitionsAndKeys = Arrays.asList(
      new BinaryTopicProduceRecord("key".getBytes(), "value".getBytes(), 0),
      new BinaryTopicProduceRecord("key2".getBytes(), "value2".getBytes(), 1),
      new BinaryTopicProduceRecord("key3".getBytes(), "value3".getBytes(), 1),
      new BinaryTopicProduceRecord("key4".getBytes(), "value4".getBytes(), 2)
  );

  private final List<BinaryTopicProduceRecord> topicRecordsWithNullValues = Arrays.asList(
      new BinaryTopicProduceRecord("key".getBytes(), (byte[]) null),
      new BinaryTopicProduceRecord("key".getBytes(), (byte[]) null),
      new BinaryTopicProduceRecord("key".getBytes(), (byte[]) null),
      new BinaryTopicProduceRecord("key".getBytes(), (byte[]) null)
  );

  // Produce to partition inputs & results
  private final List<BinaryProduceRecord> partitionRecordsOnlyValues = Arrays.asList(
      new BinaryProduceRecord("value".getBytes()),
      new BinaryProduceRecord("value2".getBytes()),
      new BinaryProduceRecord("value3".getBytes()),
      new BinaryProduceRecord("value4".getBytes())
  );

  private final List<BinaryProduceRecord> partitionRecordsWithKeys = Arrays.asList(
      new BinaryProduceRecord("key".getBytes(), "value".getBytes()),
      new BinaryProduceRecord("key".getBytes(), "value2".getBytes()),
      new BinaryProduceRecord("key".getBytes(), "value3".getBytes()),
      new BinaryProduceRecord("key".getBytes(), "value4".getBytes())
  );

  private final List<BinaryProduceRecord> partitionRecordsWithNullValues = Arrays.asList(
      new BinaryProduceRecord("key1".getBytes(), null),
      new BinaryProduceRecord("key2".getBytes(), null),
      new BinaryProduceRecord("key3".getBytes(), null),
      new BinaryProduceRecord("key4".getBytes(), null)
  );

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

  private boolean sawCallback;

  @Override
  protected ZkUtils getZkUtils(KafkaRestConfig appConfig) {
    testZkUtils = ZkUtils.apply(
        appConfig.getString(KafkaRestConfig.ZOOKEEPER_CONNECT_CONFIG), 30000, 30000,
        JaasUtils.isZkSecurityEnabled());
    return testZkUtils;
  }

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
                                      JavaConversions.asScalaBuffer(this.servers),
                                      new Properties());
  }

  // This should really be a unit test, but producer settings aren't accessible and any requests
  // trigger metadata requests, so to verify we need to use a full cluster setup
  @Test
  public void testProducerConfigOverrides() {
    Properties overrides = new Properties();
    overrides.setProperty("block.on.buffer.full", "false");
    overrides.setProperty("buffer.memory", "1");
    // Note separate ProducerPool since the override should only be for this test, so
    // getProducerPool doesn't work here
    ProducerPool pool = new ProducerPool(this.restConfig, this.brokerList, overrides);

    sawCallback = false;
    pool.produce(topicName, 0, EmbeddedFormat.BINARY, null,
                 Arrays.asList(new BinaryProduceRecord("data".getBytes())),
                 new ProducerPool.ProduceRequestCallback() {
                   @Override
                   public void onCompletion(
                       Integer keySchemaId,
                       Integer valueSchemaId,
                       List<RecordMetadataOrException> results) {
                     sawCallback = true;
                     assertNotNull(results.get(0).getException());
                     assertEquals(results.get(0).getException().getClass(),
                                  RecordTooLargeException.class);
                   }
                 });
    assertTrue(sawCallback);
  }

  @Test
  public void testProduceToTopicWithKeys() {
    testProduceToTopic(topicName, topicRecordsWithKeys, binaryDecoder, binaryDecoder,
                       produceOffsets, false);
  }

  @Test
  public void testProduceToTopicWithPartitions() {
    testProduceToTopic(topicName, topicRecordsWithPartitions, binaryDecoder, binaryDecoder,
                       producePartitionedOffsets, true);
  }

  @Test
  public void testProduceToTopicWithPartitionsAndKeys() {
    testProduceToTopic(topicName, topicRecordsWithPartitionsAndKeys, binaryDecoder, binaryDecoder,
                       producePartitionedOffsets, true);
  }

  @Test
  public void testProduceToTopicWithNullValues() {
    testProduceToTopic(topicName, topicRecordsWithNullValues, binaryDecoder, binaryDecoder,
                       produceOffsets, false);
  }

  @Test
  public void testProduceToInvalidTopic() {
    // This test turns auto-create off, so this should generate an error. Ideally it would
    // generate a 404, but Kafka as of 0.8.2.0 doesn't generate the correct exception, see
    // KAFKA-1884. For now this will just show up as failure to send all the messages.
    testProduceToTopicFails("invalid-topic", topicRecordsWithKeys);
  }


  protected void testProduceToPartition(List<? extends ProduceRecord<byte[], byte[]>> records,
                                        List<PartitionOffset> offsetResponse) {
    testProduceToPartition(topicName, 0, records, binaryDecoder, binaryDecoder, offsetResponse);
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

    List<String> versions = Arrays.asList(Versions.KAFKA_V1_JSON_BINARY, Versions.KAFKA_V1_JSON,
        Versions.KAFKA_DEFAULT_JSON, Versions.JSON, Versions.GENERIC_REQUEST,
        Versions.KAFKA_V1_JSON_AVRO, Versions.KAFKA_V2_JSON_AVRO, Versions.KAFKA_V1_JSON_JSON,
        Versions.KAFKA_V2_JSON_JSON, Versions.KAFKA_V2_JSON_BINARY, Versions.KAFKA_V2_JSON_JSON,
        Versions.KAFKA_V2_JSON_AVRO
    );

    for (String version : versions) {
      Response response = request("/topics/" + topicName)
          .post(Entity.entity(null, version));
      assertEquals("Produces to topic failed using "+ version, 422, response.getStatus());
    }

    for (String version : versions) {
      Response response = request("/topics/" + topicName+" /partitions/0")
          .post(Entity.entity(null, version));
      assertEquals("Produces to topic partition failed using "+ version,422,
          response.getStatus());
    }

  }
}
