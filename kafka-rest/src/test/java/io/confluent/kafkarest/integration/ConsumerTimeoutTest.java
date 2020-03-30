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

import io.confluent.kafkarest.Versions;
import io.confluent.kafkarest.entities.EmbeddedFormat;
import io.confluent.kafkarest.entities.v2.BinaryConsumerRecord;
import java.util.List;
import javax.ws.rs.core.GenericType;
import org.junit.Before;
import org.junit.Test;

public class ConsumerTimeoutTest extends AbstractConsumerTest {

  private static final String topicName = "test";
  private static final String groupName = "testconsumergroup";

  private static final Integer requestTimeout = 500;
  // This is pretty large since there is sometimes significant overhead to doing a read (e.g.
  // checking topic existence in ZK)
  private static final Integer instanceTimeout = 1000;
  private static final Integer slackTime = 1100;

  @Before
  @Override
  public void setUp() throws Exception {
    restProperties.setProperty("consumer.request.timeout.ms", requestTimeout.toString());
    restProperties.setProperty("consumer.instance.timeout.ms", instanceTimeout.toString());
    super.setUp();
    final int numPartitions = 3;
    final int replicationFactor = 1;
    createTopic(topicName, numPartitions, (short) replicationFactor);
  }

  @Test
  public void testConsumerTimeout() throws InterruptedException {
    String instanceUri = startConsumeMessages(groupName, topicName, EmbeddedFormat.BINARY,
                                              Versions.KAFKA_V2_JSON_BINARY);
    // Even with identical timeouts, should be able to consume multiple times without the
    // instance timing out
    consumeForTimeout(instanceUri,
                      Versions.KAFKA_V2_JSON_BINARY, Versions.KAFKA_V2_JSON_BINARY,
                      new GenericType<List<BinaryConsumerRecord>>() {
                      });
    consumeForTimeout(instanceUri,
                      Versions.KAFKA_V2_JSON_BINARY, Versions.KAFKA_V2_JSON_BINARY,
                      new GenericType<List<BinaryConsumerRecord>>() {
                      });
    // Then sleep long enough for it to expire
    Thread.sleep(instanceTimeout + slackTime);

    consumeForNotFoundError(instanceUri);
  }
}
