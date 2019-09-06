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

import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import io.confluent.kafkarest.entities.BinaryTopicProduceRecord;
import io.confluent.kafkarest.entities.PartitionOffset;

public class ProducerTopicAutoCreationTest extends AbstractProducerTest {

  private static final String topicName = "nonexistant";

  private final List<BinaryTopicProduceRecord> topicRecords = Arrays.asList(
      new BinaryTopicProduceRecord("key".getBytes(), "value".getBytes()),
      new BinaryTopicProduceRecord("key".getBytes(), "value2".getBytes()),
      new BinaryTopicProduceRecord("key".getBytes(), "value3".getBytes()),
      new BinaryTopicProduceRecord("key".getBytes(), "value4".getBytes())
  );
  private final List<PartitionOffset> partitionOffsets = Arrays.asList(
      new PartitionOffset(0, 0L, null, null),
      new PartitionOffset(0, 1L, null, null),
      new PartitionOffset(0, 2L, null, null),
      new PartitionOffset(0, 3L, null, null)
  );

  public Properties overrideBrokerProperties(int i, Properties props) {
    Properties refined = (Properties) props.clone();
    refined.setProperty("auto.create.topics.enable", "true");
    return refined;
  }

  @Test
  public void testProduceToMissingTopic() {
    // Should create topic
    testProduceToTopic(topicName, topicRecords, ByteArrayDeserializer.class.getName(),
        ByteArrayDeserializer.class.getName(), partitionOffsets, false);
  }
}
