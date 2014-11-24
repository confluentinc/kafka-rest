/**
 * Copyright 2014 Confluent Inc.
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
 */
package io.confluent.kafkarest.integration;

import kafka.utils.TestUtils;
import org.junit.Before;
import org.junit.Test;
import scala.collection.JavaConversions;

import java.util.Properties;

public class ConsumerTimeoutTest extends AbstractConsumerTest {
    private static final String topicName = "test";
    private static final String groupName = "testconsumergroup";

    private static final Integer requestTimeout = 50;
    // This is pretty large since there is sometimes significant overhead to doing a read (e.g. checking topic existence
    // in ZK)
    private static final Integer instanceTimeout = 1000;
    private static final Integer slackTime = 5;

    @Before
    @Override
    public void setUp() throws Exception {
        restProperties.setProperty("consumer.request.timeout.ms", requestTimeout.toString());
        restProperties.setProperty("consumer.instance.timeout.ms", instanceTimeout.toString());
        super.setUp();
        final int numPartitions = 3;
        final int replicationFactor = 1;
        TestUtils.createTopic(zkClient, topicName, numPartitions, replicationFactor, JavaConversions.asScalaIterable(this.servers).toSeq(), new Properties());
    }

    @Test
    public void testConsumerTimeout() throws InterruptedException {
        String instanceUri = startConsumeMessages(groupName, topicName);
        // Even with identical timeouts, should be able to consume multiple times without the instance timing out
        consumeForTimeout(instanceUri, topicName);
        consumeForTimeout(instanceUri, topicName);
        // Then sleep long enough for it to expire
        Thread.sleep(instanceTimeout + slackTime);

        consumeForNotFoundError(instanceUri, topicName);
    }
}
