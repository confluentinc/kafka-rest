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
import io.confluent.kafkarest.entities.v2.JsonConsumerRecord;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import javax.ws.rs.core.GenericType;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerJsonTest extends AbstractConsumerTest {

  private static final String topicName = "topic1";
  private static final String groupName = "testconsumergroup";

  private static final Logger log = LoggerFactory.getLogger(ConsumerJsonTest.class);

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
    return res;
  }

  private final List<ProducerRecord<Object, Object>> recordsWithKeys =
      Arrays.asList(
          new ProducerRecord<Object, Object>(topicName, "key", "value"),
          new ProducerRecord<Object, Object>(topicName, "key", null),
          new ProducerRecord<Object, Object>(topicName, "key", 43.2),
          new ProducerRecord<Object, Object>(topicName, "key", 999),
          new ProducerRecord<Object, Object>(topicName, "key", exampleMapValue()),
          new ProducerRecord<Object, Object>(topicName, "key", exampleListValue()));

  private final List<ProducerRecord<Object, Object>> recordsOnlyValues =
      Arrays.asList(
          new ProducerRecord<Object, Object>(topicName, "value"),
          new ProducerRecord<Object, Object>(topicName, null),
          new ProducerRecord<Object, Object>(topicName, 43.2),
          new ProducerRecord<Object, Object>(topicName, 999),
          new ProducerRecord<Object, Object>(topicName, exampleMapValue()),
          new ProducerRecord<Object, Object>(topicName, exampleListValue()));

  private static final GenericType<List<JsonConsumerRecord>> jsonConsumerRecordType =
      new GenericType<List<JsonConsumerRecord>>() {};

  @BeforeEach
  @Override
  public void setUp() throws Exception {
    super.setUp();
    final int numPartitions = 3;
    final int replicationFactor = 1;
    createTopic(topicName, numPartitions, (short) replicationFactor);
  }

  @Test
  public void testConsumeWithKeys() {
    String instanceUri =
        startConsumeMessages(
            groupName, topicName, EmbeddedFormat.JSON, Versions.KAFKA_V2_JSON_JSON, "earliest");
    produceJsonMessages(recordsWithKeys);
    consumeMessages(
        instanceUri,
        recordsWithKeys,
        Versions.KAFKA_V2_JSON_JSON,
        Versions.KAFKA_V2_JSON_JSON,
        jsonConsumerRecordType,
        /* converter= */ null,
        JsonConsumerRecord::toConsumerRecord);
    commitOffsets(instanceUri);
  }

  @Test
  public void testConsumeOnlyValues() {
    String instanceUri =
        startConsumeMessages(
            groupName, topicName, EmbeddedFormat.JSON, Versions.KAFKA_V2_JSON_JSON, "earliest");
    produceJsonMessages(recordsOnlyValues);
    consumeMessages(
        instanceUri,
        recordsOnlyValues,
        Versions.KAFKA_V2_JSON_JSON,
        Versions.KAFKA_V2_JSON_JSON,
        jsonConsumerRecordType,
        /* converter= */ null,
        JsonConsumerRecord::toConsumerRecord);
    commitOffsets(instanceUri);
  }

  @Test
  public void testConsumeWithMultipleParallelConsumers() throws InterruptedException {
    class ConsumerTask implements Callable<Void> {

      private int index = 0;
      private CountDownLatch doneLatch;
      private CountDownLatch readyForConsumeLoop;
      private CountDownLatch otherConsumerReadyForConsumeLoop;

      public ConsumerTask(
          int index,
          CountDownLatch doneLatch,
          CountDownLatch readyForConsumeLoop,
          CountDownLatch otherConsumerReadyForConsumeLoop) {
        this.index = index;
        this.doneLatch = doneLatch;
        this.readyForConsumeLoop = readyForConsumeLoop;
        this.otherConsumerReadyForConsumeLoop = otherConsumerReadyForConsumeLoop;
      }

      @Override
      public Void call() throws InterruptedException {
        log.info("Consumer instance {}, begin.", index);
        String instanceUri =
            startConsumeMessages(
                groupName, topicName, EmbeddedFormat.JSON, Versions.KAFKA_V2_JSON_JSON, "earliest");
        readyForConsumeLoop.countDown();
        log.info("Consumer instance {}, subscribed, ready to consume in a loop.", index);

        // Wait for other parallel consumer to be "ready", which implies it should have subscribed
        // and done consume-once.
        // The other consumer will do the same, so both after this latch will do parallel consumes.
        log.info("Consumer instance {}, wait for other consumer to be ready to consume in a loop.");
        otherConsumerReadyForConsumeLoop.await();
        log.info("Consumer instance {}, other consumer ready, will consume 20 time more.", index);
        for (int i = 0; i < 20; i++) {
          consumeMessages(
              instanceUri,
              new ArrayList<>(),
              Versions.KAFKA_V2_JSON_JSON,
              Versions.KAFKA_V2_JSON_JSON,
              jsonConsumerRecordType,
              /* converter= */ null,
              JsonConsumerRecord::toConsumerRecord);
        }

        doneLatch.countDown();
        log.info("Consumer instance {}, done.", index);

        return null;
      }
    }

    ExecutorService consumerExecutor = Executors.newFixedThreadPool(2 /* # of consumers */);

    CountDownLatch consumer0Done = new CountDownLatch(1);
    CountDownLatch consumer1Done = new CountDownLatch(1);
    CountDownLatch consumer0ReadyForConsumeLoop = new CountDownLatch(1);
    CountDownLatch consumer1ReadyForConsumeLoop = new CountDownLatch(1);
    consumerExecutor.submit(
        new ConsumerTask(
            0,
            consumer0Done,
            consumer0ReadyForConsumeLoop,
            consumer1ReadyForConsumeLoop /* otherConsumerReadyForConsumeLoop */));
    consumerExecutor.submit(
        new ConsumerTask(
            1,
            consumer1Done,
            consumer1ReadyForConsumeLoop,
            consumer0ReadyForConsumeLoop /* otherConsumerReadyForConsumeLoop */));

    consumer0Done.await();
    consumer1Done.await();
  }
}
