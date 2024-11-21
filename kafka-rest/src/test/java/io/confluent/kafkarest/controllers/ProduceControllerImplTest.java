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

import static java.util.Collections.emptyList;
import static java.util.Collections.emptySet;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.common.collect.ImmutableMultimap;
import com.google.protobuf.ByteString;
import io.confluent.kafkarest.entities.ProduceResult;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RoundRobinPartitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

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

  @BeforeEach
  public void setUp() {
    producer =
        new MockProducer<>(
            CLUSTER,
            /* autoComplete= */ false,
            new RoundRobinPartitioner(),
            new ByteArraySerializer(),
            new ByteArraySerializer());
    produceController = new ProduceControllerImpl(producer);
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
