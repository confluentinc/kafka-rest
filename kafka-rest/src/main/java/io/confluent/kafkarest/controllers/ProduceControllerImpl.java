/*
 * Copyright 2021 Confluent Inc.
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

import static java.util.Objects.requireNonNull;

import com.google.common.collect.Multimap;
import com.google.protobuf.ByteString;
import io.confluent.kafkarest.entities.ProduceResult;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.inject.Inject;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;

final class ProduceControllerImpl implements ProduceController {

  private final Producer<byte[], byte[]> producer;

  @Inject
  ProduceControllerImpl(Producer<byte[], byte[]> producer) {
    this.producer = requireNonNull(producer);
  }

  @Override
  public CompletableFuture<ProduceResult> produce(
      String clusterId,
      String topicName,
      Optional<Integer> partitionId,
      Multimap<String, Optional<ByteString>> headers,
      Optional<ByteString> key,
      Optional<ByteString> value,
      Instant timestamp
  ) {
    CompletableFuture<ProduceResult> result = new CompletableFuture<>();
    producer.send(
        new ProducerRecord<>(
            topicName,
            partitionId.orElse(null),
            timestamp.toEpochMilli(),
            key.map(ByteString::toByteArray).orElse(null),
            value.map(ByteString::toByteArray).orElse(null),
            headers.entries().stream()
                .map(
                    header ->
                        new RecordHeader(
                            header.getKey(),
                            header.getValue().map(ByteString::toByteArray).orElse(null)))
                .collect(Collectors.toList())),
        (metadata, exception) -> {
          if (exception != null) {
            result.completeExceptionally(exception);
          } else {
            result.complete(ProduceResult.fromRecordMetadata(metadata));
          }
        });
    return result;
  }
}
