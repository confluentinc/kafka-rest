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

import io.confluent.kafkarest.Versions;
import io.confluent.kafkarest.entities.EmbeddedFormat;
import io.confluent.kafkarest.entities.JsonConsumerRecord;
import kafka.utils.TestUtils;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Before;
import org.junit.Test;
import scala.collection.JavaConversions;

import javax.ws.rs.core.GenericType;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class ConsumerJsonTest extends AbstractConsumerTest {

  private static final String topicName = "topic1";
  private static final String groupName = "testconsumergroup";

  private final List<ProducerRecord<Object, Object>> recordsWithKeys = Arrays.asList(
      new ProducerRecord<Object, Object>(topicName, "key", "value"),
      new ProducerRecord<Object, Object>(topicName, "key", "value2"),
      new ProducerRecord<Object, Object>(topicName, "key", "value3"),
      new ProducerRecord<Object, Object>(topicName, "key", "value4")
  );

  private final List<ProducerRecord<Object, Object>> recordsOnlyValues = Arrays.asList(
      new ProducerRecord<Object, Object>(topicName, "value"),
      new ProducerRecord<Object, Object>(topicName, "value2"),
      new ProducerRecord<Object, Object>(topicName, "value3"),
      new ProducerRecord<Object, Object>(topicName, "value4")
  );

  private static final GenericType<List<JsonConsumerRecord>> jsonConsumerRecordType
      = new GenericType<List<JsonConsumerRecord>>() {
  };

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    final int numPartitions = 3;
    final int replicationFactor = 1;
    TestUtils.createTopic(zkClient, topicName, numPartitions, replicationFactor,
        JavaConversions.asScalaIterable(this.servers).toSeq(), new Properties());
  }

  @Test
  public void testConsumeWithKeys() {
    String instanceUri = startConsumeMessages(groupName, topicName, EmbeddedFormat.JSON,
        Versions.KAFKA_V1_JSON_JSON);
    produceJsonMessages(recordsWithKeys);
    consumeMessages(instanceUri, topicName, recordsWithKeys,
        Versions.KAFKA_V1_JSON_JSON, Versions.KAFKA_V1_JSON_JSON,
        jsonConsumerRecordType, null);
    commitOffsets(instanceUri);
  }

  @Test
  public void testConsumeOnlyValues() {
    String instanceUri = startConsumeMessages(groupName, topicName, EmbeddedFormat.JSON,
        Versions.KAFKA_V1_JSON_JSON);
    produceJsonMessages(recordsOnlyValues);
    consumeMessages(instanceUri, topicName, recordsOnlyValues,
        Versions.KAFKA_V1_JSON_JSON, Versions.KAFKA_V1_JSON_JSON,
        jsonConsumerRecordType, null);
    commitOffsets(instanceUri);
  }

}
