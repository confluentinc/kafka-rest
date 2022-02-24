/*
 * Copyright 2022 Confluent Inc.
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

package io.confluent.kafkarest.testing.fake;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

import com.google.protobuf.ByteString;
import io.confluent.kafkarest.common.KafkaFutures;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Future;
import java.util.stream.Stream;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.clients.producer.RoundRobinPartitioner;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.serialization.Serializer;

final class FakeKafkaProducer<K, V> implements Producer<K, V> {
  private final FakeKafkaCluster cluster;
  private final Serializer<K> keySerializer;
  private final Serializer<V> valueSerializer;
  private final Partitioner partitioner = new RoundRobinPartitioner();

  FakeKafkaProducer(
      FakeKafkaCluster cluster, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
    this.cluster = requireNonNull(cluster);
    this.keySerializer = requireNonNull(keySerializer);
    this.valueSerializer = requireNonNull(valueSerializer);
  }

  @Override
  public void initTransactions() {
    throw new UnsupportedOperationException("initTransactions is not supported");
  }

  @Override
  public void beginTransaction() throws ProducerFencedException {
    throw new UnsupportedOperationException("beginTransaction is not supported");
  }

  @Override
  public void sendOffsetsToTransaction(
      Map<TopicPartition, OffsetAndMetadata> offsets, String consumerGroupId) {
    throw new UnsupportedOperationException("sendOffsetsToTransaction is not supported");
  }

  @Override
  public void sendOffsetsToTransaction(
      Map<TopicPartition, OffsetAndMetadata> offsets, ConsumerGroupMetadata groupMetadata) {
    throw new UnsupportedOperationException("sendOffsetsToTransaction is not supported");
  }

  @Override
  public void commitTransaction() {
    throw new UnsupportedOperationException("commitTransaction is not supported");
  }

  @Override
  public void abortTransaction() throws ProducerFencedException {
    throw new UnsupportedOperationException("abortTransaction is not supported");
  }

  @Override
  public Future<RecordMetadata> send(ProducerRecord<K, V> record) {
    return send(record, (metadata, exception) -> {});
  }

  @Override
  public Future<RecordMetadata> send(ProducerRecord<K, V> record, Callback callback) {
    Optional<ByteString> key =
        Optional.ofNullable(record.key())
            .map(k -> ByteString.copyFrom(keySerializer.serialize(record.topic(), k)));
    Optional<ByteString> value =
        Optional.ofNullable(record.value())
            .map(v -> ByteString.copyFrom(valueSerializer.serialize(record.topic(), v)));
    int partition;
    if (record.partition() != null) {
      partition = record.partition();
    } else {
      partition =
          partitioner.partition(
              record.topic(),
              record.key(),
              key.map(ByteString::toByteArray).orElse(null),
              record.value(),
              value.map(ByteString::toByteArray).orElse(null),
              cluster.toClusterMetadata());
    }
    List<Header> headers =
        Stream.of(record.headers().toArray())
            .map(
                header ->
                    new Header(
                        header.key(),
                        Optional.ofNullable(header.value()).map(ByteString::copyFrom)))
            .collect(toImmutableList());
    Instant timestamp =
        Optional.ofNullable(record.timestamp()).map(Instant::ofEpochMilli).orElse(Instant.now());
    RecordMetadata metadata;
    Exception exception;
    try {
      metadata = cluster.produce(record.topic(), partition, headers, key, value, timestamp);
      exception = null;
    } catch (RuntimeException e) {
      metadata =
          new RecordMetadata(new TopicPartition(record.topic(), partition), -1, -1, -1, -1, -1);
      exception = e;
    }
    callback.onCompletion(metadata, exception);
    if (exception == null) {
      return KafkaFuture.completedFuture(metadata);
    } else {
      return KafkaFutures.failedFuture(exception);
    }
  }

  @Override
  public void flush() {}

  @Override
  public List<PartitionInfo> partitionsFor(String topic) {
    return cluster.getTopic(topic).getPartitions().stream()
        .map(partition -> partition.toPartitionInfo(cluster))
        .collect(toImmutableList());
  }

  @Override
  public Map<MetricName, ? extends Metric> metrics() {
    throw new UnsupportedOperationException("metrics is not supported");
  }

  @Override
  public void close() {}

  @Override
  public void close(Duration timeout) {}
}
