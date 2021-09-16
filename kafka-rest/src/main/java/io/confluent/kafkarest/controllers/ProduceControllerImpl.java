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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Multimap;
import com.google.protobuf.ByteString;
import io.confluent.kafkarest.KafkaRestConfig;
import io.confluent.kafkarest.SystemTime;
import io.confluent.kafkarest.Time;
import io.confluent.kafkarest.entities.ProduceResult;
import io.confluent.kafkarest.exceptions.RateLimitGracePeriodExceededException;
import io.confluent.kafkarest.exceptions.TooManyRequestsException;
import java.time.Instant;
import java.util.LinkedList;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.inject.Inject;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class ProduceControllerImpl implements ProduceController {

  private static final Logger log = LoggerFactory.getLogger(ProduceController.class);

  private final Producer<byte[], byte[]> producer;
  private final KafkaRestConfig config;
  private final LinkedList<Long> rateCounter = new LinkedList<>();
  private Optional<Long> gracePeriodStart = Optional.empty();
  private static final int ONE_SECOND = 1000;

  @VisibleForTesting public Time time = new SystemTime();

  @Inject
  ProduceControllerImpl(Producer<byte[], byte[]> producer, KafkaRestConfig config) {
    this.producer = requireNonNull(producer);
    this.config = config;
  }

  private void addToAndCullRateCounter(long now) {

    rateCounter.add(now);
    while (rateCounter.peek() < now - ONE_SECOND) {
      rateCounter.pop();
    }
    if (rateCounter.size() <= config.getInt(KafkaRestConfig.PRODUCE_MAX_REQUESTS_PER_SECOND)) {
      gracePeriodStart = Optional.empty();
    }
  }

  @Override
  public CompletableFuture<ProduceResult> produce(
      String clusterId,
      String topicName,
      Optional<Integer> partitionId,
      Multimap<String, Optional<ByteString>> headers,
      Optional<ByteString> key,
      Optional<ByteString> value,
      Instant timestamp) {

    long now = time.milliseconds();
    addToAndCullRateCounter(now);

    CompletableFuture<ProduceResult> result = new CompletableFuture<>();
    log.debug("Producing to kafka");
    if (rateCounter.size() > config.getInt(KafkaRestConfig.PRODUCE_MAX_REQUESTS_PER_SECOND)) {

      if (!gracePeriodStart.isPresent()
          && config.getInt(KafkaRestConfig.PRODUCE_GRACE_PERIOD) != 0) {
        gracePeriodStart = Optional.of(now);
      } else if (config.getInt(KafkaRestConfig.PRODUCE_GRACE_PERIOD) == 0
          || config.getInt(KafkaRestConfig.PRODUCE_GRACE_PERIOD) < now - gracePeriodStart.get()) {
        result.completeExceptionally(new RateLimitGracePeriodExceededException());
        return result;
      }

    } else {
      gracePeriodStart = Optional.empty();
    }

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
            log.debug("Received exception from kafka", exception);
            result.completeExceptionally(exception);
          } else if (gracePeriodStart.isPresent()) {
            result.completeExceptionally(
                new TooManyRequestsException(
                    config.getInt(KafkaRestConfig.PRODUCE_MAX_REQUESTS_PER_SECOND),
                    config.getInt(KafkaRestConfig.PRODUCE_GRACE_PERIOD)));
          } else {
            log.debug("Received response from kafka");
            result.complete(ProduceResult.fromRecordMetadata(metadata, Instant.now()));
          }
        });
    return result;
  }
}
