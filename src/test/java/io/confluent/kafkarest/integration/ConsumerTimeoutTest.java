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

import java.util.List;
import java.util.Properties;

import javax.ws.rs.core.GenericType;

import io.confluent.kafkarest.Versions;
import io.confluent.kafkarest.entities.BinaryConsumerRecord;
import io.confluent.kafkarest.entities.EmbeddedFormat;
import kafka.utils.TestUtils;
import scala.collection.JavaConversions;

public class ConsumerTimeoutTest extends AbstractConsumerTest {

  private static final String topicName = "test";
  private static final String groupName = "testconsumergroup";

  private static final Integer requestTimeout = 500;
  // This is pretty large since there is sometimes significant overhead to doing a read (e.g.
  // checking topic existence in ZK)
  private static final Integer instanceTimeout = 2500;
  private static final Integer slackTime = 5;

  @Before
  @Override
  public void setUp() throws Exception {
    restProperties.setProperty("consumer.request.timeout.ms", requestTimeout.toString());
    restProperties.setProperty("consumer.instance.timeout.ms", instanceTimeout.toString());
    super.setUp();
    final int numPartitions = 3;
    final int replicationFactor = 1;
    TestUtils.createTopic(zkUtils, topicName, numPartitions, replicationFactor,
                          JavaConversions.asScalaBuffer(this.servers), new Properties());
  }

  @Test
  public void testConsumerTimeout() throws InterruptedException {
    String instanceUri = startConsumeMessages(groupName, topicName, EmbeddedFormat.BINARY,
                                              Versions.KAFKA_V1_JSON_BINARY);
    // Even with identical timeouts, should be able to consume multiple times without the
    // instance timing out
    consumeForTimeout(instanceUri, topicName,
                      Versions.KAFKA_V1_JSON_BINARY, Versions.KAFKA_V1_JSON_BINARY,
                      new GenericType<List<BinaryConsumerRecord>>() {
                      });
    consumeForTimeout(instanceUri, topicName,
                      Versions.KAFKA_V1_JSON_BINARY, Versions.KAFKA_V1_JSON_BINARY,
                      new GenericType<List<BinaryConsumerRecord>>() {
                      });
    // Then sleep long enough for it to expire
    Thread.sleep(instanceTimeout + slackTime);

    consumeForNotFoundError(instanceUri, topicName);
  }
}
