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

import io.confluent.kafkarest.RestConfigUtils;
import io.confluent.kafkarest.Versions;
import io.confluent.kafkarest.entities.*;
import kafka.serializer.Decoder;
import kafka.serializer.DefaultDecoder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import scala.collection.JavaConversions;

import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.Response;
import java.util.*;
import java.util.stream.Collectors;

import static io.confluent.kafkarest.TestUtils.assertOKResponse;

public class ConsumerGroupsTest extends AbstractProducerTest {

  private final String topicName = "topic";
  private final String topic2 = "topic2";
  private final String groupName = "testconsumergroup";

  private final List<BinaryTopicProduceRecord> topicRecordsWithKeys = Arrays.asList(
          new BinaryTopicProduceRecord("key".getBytes(), "value".getBytes()),
          new BinaryTopicProduceRecord("key".getBytes(), "value2".getBytes()),
          new BinaryTopicProduceRecord("key".getBytes(), "value3".getBytes()),
          new BinaryTopicProduceRecord("key".getBytes(), "value4".getBytes())
  );



  private final List<PartitionOffset> produceOffsets = Arrays.asList(
          new PartitionOffset(0, 0L, null, null),
          new PartitionOffset(0, 1L, null, null),
          new PartitionOffset(0, 2L, null, null),
          new PartitionOffset(0, 3L, null, null)
  );

  private static final Decoder<byte[]> binaryDecoder = new DefaultDecoder(null);

  private final int defaultRebalancedTimeoutInMillis = 5000;

  private KafkaConsumer createNativeConsumer(String groupName, Collection<String> topics) {
    Properties properties = new Properties();
    String deserializer = StringDeserializer.class.getName();
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
            RestConfigUtils.bootstrapBrokers(restConfig));
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupName);
    properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
    properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, deserializer);
    properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, deserializer);
    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    KafkaConsumer kafkaConsumer = new KafkaConsumer<Byte[], String>(properties);
    kafkaConsumer.subscribe(topics);
    return kafkaConsumer;
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
    kafka.utils.TestUtils.createTopic(zkClient, topic2, numPartitions, replicationFactor,
            JavaConversions.asScalaBuffer(this.servers),
            new Properties());
  }

  @Test
  public void testGetConsumerGroups() throws InterruptedException {
    testProduceToTopic(topicName, topicRecordsWithKeys, ByteArrayDeserializer.class.getName(),
            ByteArrayDeserializer.class.getName(),
            produceOffsets, false);
    try (KafkaConsumer kafkaConsumer = createNativeConsumer(groupName,
            Collections.singleton(topicName))) {
      Thread.sleep(defaultRebalancedTimeoutInMillis);

      kafkaConsumer.poll(100);
      kafkaConsumer.commitSync();

      Response response = request("/groups").get();
      assertOKResponse(response, Versions.KAFKA_MOST_SPECIFIC_DEFAULT);
      List<ConsumerGroup> groups = response.readEntity(new GenericType<List<ConsumerGroup>>() {
      });
      ConsumerGroupCoordinator expectedCoordinator = groups.stream()
              .filter(g -> groupName.equals(g.getGroupId()))
              .collect(Collectors.toList()).get(0).getCoordinator();
      Assert.assertTrue(groups.contains(new ConsumerGroup(groupName, expectedCoordinator)));
    }
  }

  @Test
  public void testGetConsumerGroupMetrics() throws InterruptedException {
    testProduceToTopic(topicName, topicRecordsWithKeys, ByteArrayDeserializer.class.getName(),
            ByteArrayDeserializer.class.getName(),
            produceOffsets, false);
    try (KafkaConsumer kafkaConsumer = createNativeConsumer(groupName,
            Collections.singleton(topicName))) {
      Thread.sleep(defaultRebalancedTimeoutInMillis);

      kafkaConsumer.poll(100);
      kafkaConsumer.commitSync();

      Response response = request("/groups/"+ groupName + "/partitions").get();
      assertOKResponse(response, Versions.KAFKA_MOST_SPECIFIC_DEFAULT);
      ConsumerEntity groupInfo = response.readEntity(ConsumerEntity.class);
      Assert.assertFalse("Group information should not be null",groupInfo == null);
      Assert.assertEquals(3, groupInfo.getTopicPartitionList().size());
    }
  }

  @Test
  public void testGetConsumerGroupTopics() throws InterruptedException {
    testProduceToTopic(topicName, topicRecordsWithKeys, ByteArrayDeserializer.class.getName(),
            ByteArrayDeserializer.class.getName(),
            produceOffsets, false);
    try (KafkaConsumer kafkaConsumer = createNativeConsumer(groupName,
            Collections.singleton(topicName))) {
      Thread.sleep(defaultRebalancedTimeoutInMillis);

      kafkaConsumer.poll(100);
      kafkaConsumer.commitSync();

      Response response = request("/groups/"+ groupName + "/topics").get();
      assertOKResponse(response, Versions.KAFKA_MOST_SPECIFIC_DEFAULT);
      Set<TopicName> groupTopics = response.readEntity(new GenericType<Set<TopicName>>() {});
      Assert.assertEquals(1, groupTopics.size());
      Assert.assertEquals(Collections.singleton(new TopicName(topicName)), groupTopics);

      final int expectedSize = 1;
      Map<String, String> requestParameters = new HashMap<>();
      requestParameters.put("offset", "0");
      requestParameters.put("count", "" + expectedSize);
      Response filteredResponse = request("/groups/"+ groupName + "/topics/" + topicName,
              requestParameters).get();
      assertOKResponse(filteredResponse, Versions.KAFKA_MOST_SPECIFIC_DEFAULT);
      ConsumerEntity filteredTopicInfo = filteredResponse.readEntity(ConsumerEntity.class);
      Assert.assertFalse("Group information should not be null",filteredTopicInfo == null);
      Assert.assertEquals(expectedSize, filteredTopicInfo.getTopicPartitionList().size());
      Assert.assertEquals(6, filteredTopicInfo.getTopicPartitionCount().longValue());


      Response topicGroupResponse = request("/groups/"+ groupName + "/topics/" + topicName).get();
      assertOKResponse(topicGroupResponse, Versions.KAFKA_MOST_SPECIFIC_DEFAULT);
      ConsumerEntity nonFilteredTopicInfo = topicGroupResponse.readEntity(ConsumerEntity.class);
      Assert.assertFalse("Group information should not be null",nonFilteredTopicInfo == null);
      Assert.assertEquals(6, nonFilteredTopicInfo.getTopicPartitionList().size());
      Assert.assertEquals(6, nonFilteredTopicInfo.getTopicPartitionCount().longValue());
    }
  }

}
