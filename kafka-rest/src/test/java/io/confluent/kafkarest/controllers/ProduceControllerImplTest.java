/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.kafkarest.controllers;

import static io.confluent.kafkarest.KafkaRestConfig.PRODUCE_GRACE_PERIOD;
import static io.confluent.kafkarest.KafkaRestConfig.PRODUCE_MAX_REQUESTS_PER_SECOND;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptySet;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableMultimap;
import com.google.protobuf.ByteString;
import io.confluent.kafkarest.KafkaRestConfig;
import io.confluent.kafkarest.Time;
import io.confluent.kafkarest.entities.ProduceResult;
import io.confluent.kafkarest.exceptions.RateLimitGracePeriodExceededException;
import io.confluent.kafkarest.exceptions.TooManyRequestsException;
import io.confluent.kafkarest.mock.MockTime;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RoundRobinPartitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ProduceControllerImplTest {

  private static final Cluster CLUSTER =
      new Cluster(
          "cluster-1",
          singletonList(new Node(1, "localhost", 1234)),
          Arrays.asList(
              new PartitionInfo(
                  "topic-1",
                  0,
                  new Node(1, "localhost", 1234),
                  new Node[] {new Node(1, "localhost", 1234)},
                  new Node[] {new Node(1, "localhost", 1234)}),
              new PartitionInfo(
                  "topic-1",
                  1,
                  new Node(1, "localhost", 1234),
                  new Node[] {new Node(1, "localhost", 1234)},
                  new Node[] {new Node(1, "localhost", 1234)}),
              new PartitionInfo(
                  "topic-1",
                  2,
                  new Node(1, "localhost", 1234),
                  new Node[] {new Node(1, "localhost", 1234)},
                  new Node[] {new Node(1, "localhost", 1234)})),
          emptySet(),
          emptySet());

  private static final Function<
          ProducerRecord<byte[], byte[]>, ProducerRecord<ByteString, ByteString>>
      BYTE_ARRAY_TO_BYTE_STRING_RECORD_FUNCTION =
          record ->
              new ProducerRecord<>(
                  record.topic(),
                  record.partition(),
                  record.timestamp(),
                  record.key() != null ? ByteString.copyFrom(record.key()) : null,
                  record.value() != null ? ByteString.copyFrom(record.value()) : null,
                  record.headers());

  private MockProducer<byte[], byte[]> producer;
  private ProduceController produceController;

  @Before
  public void setUp() {
    producer =
        new MockProducer<>(
            CLUSTER,
            /* autoComplete= */ false,
            new RoundRobinPartitioner(),
            new ByteArraySerializer(),
            new ByteArraySerializer());
    produceController = new ProduceControllerImpl(producer, new KafkaRestConfig());
  }

  @Test
  public void produceWithPartitionIdKeyAndValue_produces() {
    CompletableFuture<ProduceResult> result1 =
        produceController.produce(
            "cluster-1",
            "topic-1",
            /* partitionId= */ Optional.of(1),
            /* headers= */ ImmutableMultimap.of(),
            Optional.of(ByteString.copyFromUtf8("key-1")),
            Optional.of(ByteString.copyFromUtf8("value-1")),
            Instant.ofEpochMilli(1000));
    CompletableFuture<ProduceResult> result2 =
        produceController.produce(
            "cluster-1",
            "topic-1",
            /* partitionId= */ Optional.of(1),
            /* headers= */ ImmutableMultimap.of(),
            Optional.of(ByteString.copyFromUtf8("key-2")),
            Optional.of(ByteString.copyFromUtf8("value-2")),
            Instant.ofEpochMilli(2000));
    CompletableFuture<ProduceResult> result3 =
        produceController.produce(
            "cluster-1",
            "topic-1",
            /* partitionId= */ Optional.of(1),
            /* headers= */ ImmutableMultimap.of(),
            Optional.of(ByteString.copyFromUtf8("key-3")),
            Optional.of(ByteString.copyFromUtf8("value-3")),
            Instant.ofEpochMilli(3000));

    producer.completeNext();
    producer.completeNext();
    producer.completeNext();

    // MockProducer does not set timestamp, serializedKeySize, and serializedValueSize,
    // so we only need to confirm offset and partition.
    int expectedPartition = 1;
    assertEquals(expectedPartition, result1.join().getPartitionId());
    assertEquals(0, result1.join().getOffset());
    assertEquals(expectedPartition, result2.join().getPartitionId());
    assertEquals(1, result2.join().getOffset());
    assertEquals(expectedPartition, result3.join().getPartitionId());
    assertEquals(2, result3.join().getOffset());

    assertProducerRecordsEquals(
        Arrays.asList(
            new ProducerRecord<>(
                "topic-1",
                1,
                1000L,
                "key-1".getBytes(StandardCharsets.UTF_8),
                "value-1".getBytes(StandardCharsets.UTF_8),
                /* headers= */ emptyList()),
            new ProducerRecord<>(
                "topic-1",
                1,
                2000L,
                "key-2".getBytes(StandardCharsets.UTF_8),
                "value-2".getBytes(StandardCharsets.UTF_8),
                /* headers= */ emptyList()),
            new ProducerRecord<>(
                "topic-1",
                1,
                3000L,
                "key-3".getBytes(StandardCharsets.UTF_8),
                "value-3".getBytes(StandardCharsets.UTF_8),
                /* headers= */ emptyList())),
        producer.history());
  }

  @Test
  public void produceWithNullPartitionIdKeyAndValue_produces() {
    CompletableFuture<ProduceResult> result1 =
        produceController.produce(
            "cluster-1",
            "topic-1",
            /* partitionId= */ Optional.empty(),
            /* headers= */ ImmutableMultimap.of(),
            Optional.of(ByteString.copyFromUtf8("key-1")),
            Optional.of(ByteString.copyFromUtf8("value-1")),
            Instant.ofEpochMilli(1000));
    CompletableFuture<ProduceResult> result2 =
        produceController.produce(
            "cluster-1",
            "topic-1",
            /* partitionId= */ Optional.empty(),
            /* headers= */ ImmutableMultimap.of(),
            Optional.of(ByteString.copyFromUtf8("key-2")),
            Optional.of(ByteString.copyFromUtf8("value-2")),
            Instant.ofEpochMilli(2000));
    CompletableFuture<ProduceResult> result3 =
        produceController.produce(
            "cluster-1",
            "topic-1",
            /* partitionId= */ Optional.empty(),
            /* headers= */ ImmutableMultimap.of(),
            Optional.of(ByteString.copyFromUtf8("key-3")),
            Optional.of(ByteString.copyFromUtf8("value-3")),
            Instant.ofEpochMilli(3000));

    producer.completeNext();
    producer.completeNext();
    producer.completeNext();

    // MockProducer does not set timestamp, serializedKeySize, and serializedValueSize,
    // so we only need to confirm offset and partition.
    int expectedOffset = 0;
    assertEquals(0, result1.join().getPartitionId());
    assertEquals(expectedOffset, result1.join().getOffset());
    assertEquals(1, result2.join().getPartitionId());
    assertEquals(expectedOffset, result2.join().getOffset());
    assertEquals(2, result3.join().getPartitionId());
    assertEquals(expectedOffset, result3.join().getOffset());

    assertProducerRecordsEquals(
        Arrays.asList(
            new ProducerRecord<>(
                "topic-1",
                /* partition= */ null,
                1000L,
                "key-1".getBytes(StandardCharsets.UTF_8),
                "value-1".getBytes(StandardCharsets.UTF_8),
                /* headers= */ emptyList()),
            new ProducerRecord<>(
                "topic-1",
                /* partition= */ null,
                2000L,
                "key-2".getBytes(StandardCharsets.UTF_8),
                "value-2".getBytes(StandardCharsets.UTF_8),
                /* headers= */ emptyList()),
            new ProducerRecord<>(
                "topic-1",
                /* partition= */ null,
                3000L,
                "key-3".getBytes(StandardCharsets.UTF_8),
                "value-3".getBytes(StandardCharsets.UTF_8),
                /* headers= */ emptyList())),
        producer.history());
  }

  @Test
  public void produceWithPartitionIdNullKeyAndValue_produces() {
    CompletableFuture<ProduceResult> result1 =
        produceController.produce(
            "cluster-1",
            "topic-1",
            /* partitionId= */ Optional.of(1),
            /* headers= */ ImmutableMultimap.of(),
            /* key= */ Optional.empty(),
            Optional.of(ByteString.copyFromUtf8("value-1")),
            Instant.ofEpochMilli(1000));
    CompletableFuture<ProduceResult> result2 =
        produceController.produce(
            "cluster-1",
            "topic-1",
            /* partitionId= */ Optional.of(1),
            /* headers= */ ImmutableMultimap.of(),
            /* key= */ Optional.empty(),
            Optional.of(ByteString.copyFromUtf8("value-2")),
            Instant.ofEpochMilli(2000));
    CompletableFuture<ProduceResult> result3 =
        produceController.produce(
            "cluster-1",
            "topic-1",
            /* partitionId= */ Optional.of(1),
            /* headers= */ ImmutableMultimap.of(),
            /* key= */ Optional.empty(),
            Optional.of(ByteString.copyFromUtf8("value-3")),
            Instant.ofEpochMilli(3000));

    producer.completeNext();
    producer.completeNext();
    producer.completeNext();

    // MockProducer does not set timestamp, serializedKeySize, and serializedValueSize,
    // so we only need to confirm offset and partition.
    int expectedPartition = 1;
    assertEquals(expectedPartition, result1.join().getPartitionId());
    assertEquals(0, result1.join().getOffset());
    assertEquals(expectedPartition, result2.join().getPartitionId());
    assertEquals(1, result2.join().getOffset());
    assertEquals(expectedPartition, result3.join().getPartitionId());
    assertEquals(2, result3.join().getOffset());

    assertProducerRecordsEquals(
        Arrays.asList(
            new ProducerRecord<>(
                "topic-1",
                1,
                1000L,
                /* key= */ null,
                "value-1".getBytes(StandardCharsets.UTF_8),
                /* headers= */ emptyList()),
            new ProducerRecord<>(
                "topic-1",
                1,
                2000L,
                /* key= */ null,
                "value-2".getBytes(StandardCharsets.UTF_8),
                /* headers= */ emptyList()),
            new ProducerRecord<>(
                "topic-1",
                1,
                3000L,
                /* key= */ null,
                "value-3".getBytes(StandardCharsets.UTF_8),
                /* headers= */ emptyList())),
        producer.history());
  }

  @Test
  public void produceWithPartitionIdKeyAndNullValue_produces() {
    CompletableFuture<ProduceResult> result1 =
        produceController.produce(
            "cluster-1",
            "topic-1",
            /* partitionId= */ Optional.of(1),
            /* headers= */ ImmutableMultimap.of(),
            Optional.of(ByteString.copyFromUtf8("key-1")),
            /* value= */ Optional.empty(),
            Instant.ofEpochMilli(1000));
    CompletableFuture<ProduceResult> result2 =
        produceController.produce(
            "cluster-1",
            "topic-1",
            /* partitionId= */ Optional.of(1),
            /* headers= */ ImmutableMultimap.of(),
            Optional.of(ByteString.copyFromUtf8("key-2")),
            /* value= */ Optional.empty(),
            Instant.ofEpochMilli(2000));
    CompletableFuture<ProduceResult> result3 =
        produceController.produce(
            "cluster-1",
            "topic-1",
            /* partitionId= */ Optional.of(1),
            /* headers= */ ImmutableMultimap.of(),
            Optional.of(ByteString.copyFromUtf8("key-3")),
            /* value= */ Optional.empty(),
            Instant.ofEpochMilli(3000));

    producer.completeNext();
    producer.completeNext();
    producer.completeNext();

    // MockProducer does not set timestamp, serializedKeySize, and serializedValueSize,
    // so we only need to confirm offset and partition.
    int expectedPartition = 1;
    assertEquals(expectedPartition, result1.join().getPartitionId());
    assertEquals(0, result1.join().getOffset());
    assertEquals(expectedPartition, result2.join().getPartitionId());
    assertEquals(1, result2.join().getOffset());
    assertEquals(expectedPartition, result3.join().getPartitionId());
    assertEquals(2, result3.join().getOffset());

    assertProducerRecordsEquals(
        Arrays.asList(
            new ProducerRecord<>(
                "topic-1",
                1,
                1000L,
                "key-1".getBytes(StandardCharsets.UTF_8),
                /* value= */ null,
                /* headers= */ emptyList()),
            new ProducerRecord<>(
                "topic-1",
                1,
                2000L,
                "key-2".getBytes(StandardCharsets.UTF_8),
                /* value= */ null,
                /* headers= */ emptyList()),
            new ProducerRecord<>(
                "topic-1",
                1,
                3000L,
                "key-3".getBytes(StandardCharsets.UTF_8),
                /* value= */ null,
                /* headers= */ emptyList())),
        producer.history());
  }

  @Test
  public void produceWithKeyAndValueAndHeaders_produces() {
    CompletableFuture<ProduceResult> result1 =
        produceController.produce(
            "cluster-1",
            "topic-1",
            /* partitionId= */ Optional.empty(),
            ImmutableMultimap.of("X", Optional.of(ByteString.copyFromUtf8("X"))),
            Optional.of(ByteString.copyFromUtf8("key-1")),
            Optional.of(ByteString.copyFromUtf8("value-1")),
            Instant.ofEpochMilli(1000));
    CompletableFuture<ProduceResult> result2 =
        produceController.produce(
            "cluster-1",
            "topic-1",
            /* partitionId= */ Optional.empty(),
            ImmutableMultimap.of("Y", Optional.of(ByteString.copyFromUtf8("Y"))),
            Optional.of(ByteString.copyFromUtf8("key-2")),
            Optional.of(ByteString.copyFromUtf8("value-2")),
            Instant.ofEpochMilli(2000));
    CompletableFuture<ProduceResult> result3 =
        produceController.produce(
            "cluster-1",
            "topic-1",
            /* partitionId= */ Optional.empty(),
            ImmutableMultimap.of("Z", Optional.of(ByteString.copyFromUtf8("Z"))),
            Optional.of(ByteString.copyFromUtf8("key-3")),
            Optional.of(ByteString.copyFromUtf8("value-3")),
            Instant.ofEpochMilli(3000));

    producer.completeNext();
    producer.completeNext();
    producer.completeNext();

    // MockProducer does not set timestamp, serializedKeySize, and serializedValueSize,
    // so we only need to confirm offset and partition.
    int expectedOffset = 0;
    assertEquals(0, result1.join().getPartitionId());
    assertEquals(expectedOffset, result1.join().getOffset());
    assertEquals(1, result2.join().getPartitionId());
    assertEquals(expectedOffset, result2.join().getOffset());
    assertEquals(2, result3.join().getPartitionId());
    assertEquals(expectedOffset, result3.join().getOffset());

    assertProducerRecordsEquals(
        Arrays.asList(
            new ProducerRecord<>(
                "topic-1",
                /* partition= */ null,
                1000L,
                "key-1".getBytes(StandardCharsets.UTF_8),
                "value-1".getBytes(StandardCharsets.UTF_8),
                singletonList(new RecordHeader("X", ByteString.copyFromUtf8("X").toByteArray()))),
            new ProducerRecord<>(
                "topic-1",
                /* partition= */ null,
                2000L,
                "key-2".getBytes(StandardCharsets.UTF_8),
                "value-2".getBytes(StandardCharsets.UTF_8),
                singletonList(new RecordHeader("Y", ByteString.copyFromUtf8("Y").toByteArray()))),
            new ProducerRecord<>(
                "topic-1",
                /* partition= */ null,
                3000L,
                "key-3".getBytes(StandardCharsets.UTF_8),
                "value-3".getBytes(StandardCharsets.UTF_8),
                singletonList(new RecordHeader("Z", ByteString.copyFromUtf8("Z").toByteArray())))),
        producer.history());
  }

  @Test
  public void produceWithZeroGracePeriod() {

    Properties properties = new Properties();
    properties.put(PRODUCE_GRACE_PERIOD, "0");
    properties.put(PRODUCE_MAX_REQUESTS_PER_SECOND, "1");

    Time time = new MockTime();
    produceController = new ProduceControllerImpl(producer, new KafkaRestConfig(properties));
    ((ProduceControllerImpl) produceController).rateCounter.clear();

    ((ProduceControllerImpl) produceController).time = time;

    CompletableFuture<ProduceResult> result1 =
        produceController.produce(
            "cluster-1",
            "topic-1",
            /* partitionId= */ Optional.of(1),
            /* headers= */ ImmutableMultimap.of(),
            Optional.of(ByteString.copyFromUtf8("key-1")),
            Optional.of(ByteString.copyFromUtf8("value-1")),
            Instant.ofEpochMilli(1000));

    producer.completeNext();

    AtomicInteger checkpoint1 = new AtomicInteger(0);
    assertFalse(result1.isCompletedExceptionally());
    result1.handle(
        (result, error) -> {
          assertNull(error);
          assertEquals(ProduceResult.create(1, 0, null, 0, 0), result);
          checkpoint1.incrementAndGet();
          return true;
        });
    assertEquals(new AtomicInteger(1).get(), checkpoint1.get());

    time.sleep(2);

    CompletableFuture<ProduceResult> result2 =
        produceController.produce(
            "cluster-1",
            "topic-1",
            /* partitionId= */ Optional.of(1),
            /* headers= */ ImmutableMultimap.of(),
            Optional.of(ByteString.copyFromUtf8("key-2")),
            Optional.of(ByteString.copyFromUtf8("value-2")),
            Instant.ofEpochMilli(2000));
    producer.completeNext();
    AtomicInteger checkpoint2 = new AtomicInteger(0);
    assertTrue(result2.isCompletedExceptionally());
    result2.handle(
        (result, error) -> {
          assertNull(result);
          assertEquals(RateLimitGracePeriodExceededException.class, error.getClass());
          checkpoint2.incrementAndGet();
          return true;
        });
    assertEquals(new AtomicInteger(1).get(), checkpoint2.get());

    time.sleep(1001);
    CompletableFuture<ProduceResult> result3 =
        produceController.produce(
            "cluster-1",
            "topic-1",
            /* partitionId= */ Optional.of(1),
            /* headers= */ ImmutableMultimap.of(),
            Optional.of(ByteString.copyFromUtf8("key-3")),
            Optional.of(ByteString.copyFromUtf8("value-3")),
            Instant.ofEpochMilli(3000));
    producer.completeNext();
    AtomicInteger checkpoint3 = new AtomicInteger(0);
    assertFalse(result3.isCompletedExceptionally());
    result3.handle(
        (result, error) -> {
          assertNull(error);
          assertEquals(ProduceResult.create(1, 1, null, 0, 0), result);
          checkpoint3.incrementAndGet();
          return true;
        });
    assertEquals(new AtomicInteger(1).get(), checkpoint3.get());

    assertProducerRecordsEquals(
        Arrays.asList(
            new ProducerRecord<>(
                "topic-1",
                1,
                1000L,
                "key-1".getBytes(StandardCharsets.UTF_8),
                "value-1".getBytes(StandardCharsets.UTF_8),
                /* headers= */ emptyList()),
            new ProducerRecord<>(
                "topic-1",
                1,
                3000L,
                "key-3".getBytes(StandardCharsets.UTF_8),
                "value-3".getBytes(StandardCharsets.UTF_8),
                /* headers= */ emptyList())),
        producer.history());
  }

  @Test
  public void produceWithZeroRateLimit() {
    Properties properties = new Properties();
    properties.put(PRODUCE_GRACE_PERIOD, "10");
    properties.put(PRODUCE_MAX_REQUESTS_PER_SECOND, "0");

    boolean checkpoint = false;
    try {
      produceController = new ProduceControllerImpl(producer, new KafkaRestConfig(properties));
    } catch (ConfigException ce) {
      checkpoint = true;
    }
    assertTrue(checkpoint);
  }

  @Test
  public void twoProducers() {

    Properties properties = new Properties();

    int rateLimit = 2;
    int gracePeriod = 10;

    properties.put(PRODUCE_MAX_REQUESTS_PER_SECOND, Integer.toString(rateLimit));
    properties.put(PRODUCE_GRACE_PERIOD, Integer.toString(gracePeriod));

    produceController = new ProduceControllerImpl(producer, new KafkaRestConfig(properties));
    ((ProduceControllerImpl) produceController).rateCounter.clear();
    ((ProduceControllerImpl) produceController).gracePeriodStart = Optional.empty();

    MockProducer<byte[], byte[]> producer2 =
        new MockProducer<>(
            CLUSTER,
            /* autoComplete= */ false,
            new RoundRobinPartitioner(),
            new ByteArraySerializer(),
            new ByteArraySerializer());
    ProduceController produceController2 =
        new ProduceControllerImpl(producer2, new KafkaRestConfig(properties));

    Time time = new MockTime();
    ((ProduceControllerImpl) produceController).time = time;
    ((ProduceControllerImpl) produceController2).time = time;

    CompletableFuture<ProduceResult> result1 =
        produceController.produce(
            "cluster-1",
            "topic-1",
            /* partitionId= */ Optional.of(1),
            /* headers= */ ImmutableMultimap.of(),
            Optional.of(ByteString.copyFromUtf8("key-1")),
            Optional.of(ByteString.copyFromUtf8("value-1")),
            Instant.ofEpochMilli(1000));
    producer.completeNext();

    AtomicInteger checkpoint1 = new AtomicInteger(0);
    assertFalse(result1.isCompletedExceptionally());
    result1.handle(
        (result, error) -> {
          assertNull(error);
          assertEquals(result, ProduceResult.create(1, 0, null, 0, 0));
          checkpoint1.incrementAndGet();
          return true;
        });
    assertEquals(new AtomicInteger(1).get(), checkpoint1.get());

    time.sleep(1);
    CompletableFuture<ProduceResult> result2 =
        produceController2.produce(
            "cluster-1",
            "topic-1",
            /* partitionId= */ Optional.of(1),
            /* headers= */ ImmutableMultimap.of(),
            Optional.of(ByteString.copyFromUtf8("key-2")),
            Optional.of(ByteString.copyFromUtf8("value-2")),
            Instant.ofEpochMilli(2000));
    producer2.completeNext();

    AtomicInteger checkpoint2 = new AtomicInteger(0);
    assertFalse(result2.isCompletedExceptionally());
    result2.handle(
        (result, error) -> {
          assertNull(error);
          assertEquals(result, ProduceResult.create(1, 0, null, 0, 0));
          checkpoint2.incrementAndGet();
          return true;
        });
    assertEquals(new AtomicInteger(1).get(), checkpoint2.get());

    time.sleep(1);
    CompletableFuture<ProduceResult> result3 =
        produceController2.produce(
            "cluster-1",
            "topic-1",
            /* partitionId= */ Optional.of(1),
            /* headers= */ ImmutableMultimap.of(),
            Optional.of(ByteString.copyFromUtf8("key-3")),
            Optional.of(ByteString.copyFromUtf8("value-3")),
            Instant.ofEpochMilli(3000));
    producer2.completeNext();

    AtomicInteger checkpoint3 = new AtomicInteger(0);
    assertTrue(result3.isCompletedExceptionally());
    result3.handle(
        (result, error) -> {
          assertNull(result);
          assertEquals(TooManyRequestsException.class, error.getClass());
          checkpoint3.incrementAndGet();
          return true;
        });
    assertEquals(new AtomicInteger(1).get(), checkpoint3.get());

    time.sleep(11);
    CompletableFuture<ProduceResult> result4 =
        produceController.produce(
            "cluster-1",
            "topic-1",
            /* partitionId= */ Optional.of(1),
            /* headers= */ ImmutableMultimap.of(),
            Optional.of(ByteString.copyFromUtf8("key-4")),
            Optional.of(ByteString.copyFromUtf8("value-4")),
            Instant.ofEpochMilli(4000));
    producer.completeNext();
    AtomicInteger checkpoint4 = new AtomicInteger(0);
    assertTrue(result3.isCompletedExceptionally());
    result4.handle(
        (result, error) -> {
          assertNull(result);
          assertEquals(RateLimitGracePeriodExceededException.class, error.getClass());
          checkpoint4.incrementAndGet();
          return true;
        });
    assertEquals(new AtomicInteger(1).get(), checkpoint4.get());

    assertProducerRecordsEquals(
        Arrays.asList(
            new ProducerRecord<>(
                "topic-1",
                1,
                1000L,
                "key-1".getBytes(StandardCharsets.UTF_8),
                "value-1".getBytes(StandardCharsets.UTF_8),
                /* headers= */ emptyList())),
        producer.history());

    assertProducerRecordsEquals(
        Arrays.asList(
            new ProducerRecord<>(
                "topic-1",
                1,
                2000L,
                "key-2".getBytes(StandardCharsets.UTF_8),
                "value-2".getBytes(StandardCharsets.UTF_8),
                /* headers= */ emptyList()),
            new ProducerRecord<>(
                "topic-1",
                1,
                3000L,
                "key-3".getBytes(StandardCharsets.UTF_8),
                "value-3".getBytes(StandardCharsets.UTF_8),
                /* headers= */ emptyList())),
        producer2.history());
  }

  @Test
  public void produceCombinationsHittingRateAndGraceLimit() {

    Properties properties = new Properties();

    int rateLimit = 1;
    int gracePeriod = 10;

    properties.put(PRODUCE_MAX_REQUESTS_PER_SECOND, Integer.toString(rateLimit));
    properties.put(PRODUCE_GRACE_PERIOD, Integer.toString(gracePeriod));

    produceController = new ProduceControllerImpl(producer, new KafkaRestConfig(properties));
    ((ProduceControllerImpl) produceController).rateCounter.clear();
    ((ProduceControllerImpl) produceController).gracePeriodStart = Optional.empty();

    Time time = new MockTime();
    ((ProduceControllerImpl) produceController).time = time;
    CompletableFuture<ProduceResult> result1 =
        produceController.produce(
            "cluster-1",
            "topic-1",
            /* partitionId= */ Optional.of(1),
            /* headers= */ ImmutableMultimap.of(),
            Optional.of(ByteString.copyFromUtf8("key-1")),
            Optional.of(ByteString.copyFromUtf8("value-1")),
            Instant.ofEpochMilli(1000));
    producer.completeNext();
    AtomicInteger checkpoint1 = new AtomicInteger(0);
    assertFalse(result1.isCompletedExceptionally());
    result1.handle(
        (result, error) -> {
          assertNull(error);
          assertEquals(result, ProduceResult.create(1, 0, null, 0, 0));
          checkpoint1.incrementAndGet();
          return true;
        });
    assertEquals(new AtomicInteger(1).get(), checkpoint1.get());

    time.sleep(5);
    CompletableFuture<ProduceResult> result2 =
        produceController.produce(
            "cluster-1",
            "topic-1",
            /* partitionId= */ Optional.of(1),
            /* headers= */ ImmutableMultimap.of(),
            Optional.of(ByteString.copyFromUtf8("key-2")),
            Optional.of(ByteString.copyFromUtf8("value-2")),
            Instant.ofEpochMilli(2000));
    producer.completeNext();
    AtomicInteger checkpoint2 = new AtomicInteger(0);
    assertTrue(result2.isCompletedExceptionally());
    result2.handle(
        (result, error) -> {
          assertNull(result);
          assertEquals(TooManyRequestsException.class, error.getClass());
          checkpoint2.incrementAndGet();
          return true;
        });
    assertEquals(new AtomicInteger(1).get(), checkpoint2.get());

    time.sleep(1001);
    CompletableFuture<ProduceResult> result3 =
        produceController.produce(
            "cluster-1",
            "topic-1",
            /* partitionId= */ Optional.of(1),
            /* headers= */ ImmutableMultimap.of(),
            Optional.of(ByteString.copyFromUtf8("key-3")),
            Optional.of(ByteString.copyFromUtf8("value-3")),
            Instant.ofEpochMilli(3000));
    producer.completeNext();
    AtomicInteger checkpoint3 = new AtomicInteger(0);
    assertFalse(result3.isCompletedExceptionally());
    result3.handle(
        (result, error) -> {
          assertNull(error);
          assertEquals(ProduceResult.create(1, 2, null, 0, 0), result);
          checkpoint3.incrementAndGet();
          return true;
        });
    assertEquals(new AtomicInteger(1).get(), checkpoint3.get());

    time.sleep(2);
    CompletableFuture<ProduceResult> result4 =
        produceController.produce(
            "cluster-1",
            "topic-1",
            /* partitionId= */ Optional.of(1),
            /* headers= */ ImmutableMultimap.of(),
            Optional.of(ByteString.copyFromUtf8("key-4")),
            Optional.of(ByteString.copyFromUtf8("value-4")),
            Instant.ofEpochMilli(4000));
    producer.completeNext();
    AtomicInteger checkpoint4 = new AtomicInteger(0);
    assertTrue(result4.isCompletedExceptionally());
    result4.handle(
        (result, error) -> {
          assertNull(result);
          assertEquals(TooManyRequestsException.class, error.getClass());
          checkpoint4.incrementAndGet();
          return true;
        });
    assertEquals(new AtomicInteger(1).get(), checkpoint4.get());

    time.sleep(5);
    CompletableFuture<ProduceResult> result5 =
        produceController.produce(
            "cluster-1",
            "topic-1",
            /* partitionId= */ Optional.of(1),
            /* headers= */ ImmutableMultimap.of(),
            Optional.of(ByteString.copyFromUtf8("key-5")),
            Optional.of(ByteString.copyFromUtf8("value-5")),
            Instant.ofEpochMilli(5000));
    producer.completeNext();
    AtomicInteger checkpoint5 = new AtomicInteger(0);
    assertTrue(result5.isCompletedExceptionally());
    result5.handle(
        (result, error) -> {
          assertNull(result);
          assertEquals(TooManyRequestsException.class, error.getClass());
          checkpoint5.incrementAndGet();
          return true;
        });
    assertEquals(new AtomicInteger(1).get(), checkpoint5.get());

    time.sleep(6);
    CompletableFuture<ProduceResult> result6 =
        produceController.produce(
            "cluster-1",
            "topic-1",
            /* partitionId= */ Optional.of(1),
            /* headers= */ ImmutableMultimap.of(),
            Optional.of(ByteString.copyFromUtf8("key-6")),
            Optional.of(ByteString.copyFromUtf8("value-6")),
            Instant.ofEpochMilli(6000));
    producer.completeNext();
    AtomicInteger checkpoint6 = new AtomicInteger(0);
    assertTrue(result6.isCompletedExceptionally());
    result6.handle(
        (result, error) -> {
          assertNull(result);
          assertEquals(RateLimitGracePeriodExceededException.class, error.getClass());
          checkpoint6.incrementAndGet();
          return true;
        });
    assertEquals(new AtomicInteger(1).get(), checkpoint6.get());

    time.sleep(5);
    CompletableFuture<ProduceResult> result7 =
        produceController.produce(
            "cluster-1",
            "topic-1",
            /* partitionId= */ Optional.of(1),
            /* headers= */ ImmutableMultimap.of(),
            Optional.of(ByteString.copyFromUtf8("key-7")),
            Optional.of(ByteString.copyFromUtf8("value-7")),
            Instant.ofEpochMilli(7000));
    producer.completeNext();
    AtomicInteger checkpoint7 = new AtomicInteger(0);
    assertTrue(result7.isCompletedExceptionally());
    result7.handle(
        (result, error) -> {
          assertNull(result);
          assertEquals(RateLimitGracePeriodExceededException.class, error.getClass());
          checkpoint7.incrementAndGet();
          return true;
        });
    assertEquals(new AtomicInteger(1).get(), checkpoint7.get());

    time.sleep(1001);
    CompletableFuture<ProduceResult> result8 =
        produceController.produce(
            "cluster-1",
            "topic-1",
            /* partitionId= */ Optional.of(1),
            /* headers= */ ImmutableMultimap.of(),
            Optional.of(ByteString.copyFromUtf8("key-8")),
            Optional.of(ByteString.copyFromUtf8("value-8")),
            Instant.ofEpochMilli(8000));
    producer.completeNext();
    AtomicInteger checkpoint8 = new AtomicInteger(0);
    assertFalse(result8.isCompletedExceptionally());
    result8.handle(
        (result, error) -> {
          assertNull(error);
          assertEquals(ProduceResult.create(1, 5, null, 0, 0), result);
          checkpoint8.incrementAndGet();
          return true;
        });
    assertEquals(new AtomicInteger(1).get(), checkpoint8.get());

    assertProducerRecordsEquals(
        Arrays.asList(
            new ProducerRecord<>(
                "topic-1",
                1,
                1000L,
                "key-1".getBytes(StandardCharsets.UTF_8),
                "value-1".getBytes(StandardCharsets.UTF_8),
                /* headers= */ emptyList()),
            new ProducerRecord<>(
                "topic-1",
                1,
                2000L,
                "key-2".getBytes(StandardCharsets.UTF_8),
                "value-2".getBytes(StandardCharsets.UTF_8),
                /* headers= */ emptyList()),
            new ProducerRecord<>(
                "topic-1",
                1,
                3000L,
                "key-3".getBytes(StandardCharsets.UTF_8),
                "value-3".getBytes(StandardCharsets.UTF_8),
                /* headers= */ emptyList()),
            new ProducerRecord<>(
                "topic-1",
                1,
                4000L,
                "key-4".getBytes(StandardCharsets.UTF_8),
                "value-4".getBytes(StandardCharsets.UTF_8),
                /* headers= */ emptyList()),
            new ProducerRecord<>(
                "topic-1",
                1,
                5000L,
                "key-5".getBytes(StandardCharsets.UTF_8),
                "value-5".getBytes(StandardCharsets.UTF_8),
                /* headers= */ emptyList()),
            new ProducerRecord<>(
                "topic-1",
                1,
                8000L,
                "key-8".getBytes(StandardCharsets.UTF_8),
                "value-8".getBytes(StandardCharsets.UTF_8),
                /* headers= */ emptyList())),
        producer.history());
  }

  private static void assertProducerRecordsEquals(
      List<ProducerRecord<byte[], byte[]>> expected, List<ProducerRecord<byte[], byte[]>> actual) {
    assertEquals(
        expected.stream()
            .map(BYTE_ARRAY_TO_BYTE_STRING_RECORD_FUNCTION)
            .collect(Collectors.toList()),
        actual.stream()
            .map(BYTE_ARRAY_TO_BYTE_STRING_RECORD_FUNCTION)
            .collect(Collectors.toList()));
  }
}
