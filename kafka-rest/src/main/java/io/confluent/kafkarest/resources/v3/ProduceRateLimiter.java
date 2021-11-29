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

package io.confluent.kafkarest.resources.v3;

import static java.util.Objects.requireNonNull;

import com.google.common.annotations.VisibleForTesting;
import io.confluent.kafkarest.config.ConfigModule.ProduceGracePeriodConfig;
import io.confluent.kafkarest.config.ConfigModule.ProduceRateLimitBytesConfig;
import io.confluent.kafkarest.config.ConfigModule.ProduceRateLimitCountConfig;
import io.confluent.kafkarest.config.ConfigModule.ProduceRateLimitEnabledConfig;
import io.confluent.kafkarest.exceptions.RateLimitGracePeriodExceededException;
import java.time.Clock;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import javax.inject.Inject;

final class ProduceRateLimiter {

  private static final int ONE_SECOND_MS = 1000;

  private final int maxRequestsPerSecond;
  private final int maxBytesPerSecond;
  private final long gracePeriod;
  private final boolean rateLimitingEnabled;
  private final Clock clock;
  private final AtomicInteger rateCounterSize = new AtomicInteger(0);
  private final AtomicLong byteCounterSize = new AtomicLong(0);

  private final ConcurrentLinkedDeque<TimeAndSize> rateCounter = new ConcurrentLinkedDeque<>();
  private final AtomicLong gracePeriodStart = new AtomicLong(-1);

  @Inject
  ProduceRateLimiter(
      @ProduceGracePeriodConfig Duration produceGracePeriodConfig,
      @ProduceRateLimitCountConfig Integer produceRateLimitCountConfig,
      @ProduceRateLimitBytesConfig Integer produceRateLimitBytesConfig,
      @ProduceRateLimitEnabledConfig Boolean produceRateLimitEnabledConfig,
      Clock clock) {
    this.maxRequestsPerSecond = requireNonNull(produceRateLimitCountConfig);
    this.maxBytesPerSecond = requireNonNull(produceRateLimitBytesConfig);
    this.gracePeriod = produceGracePeriodConfig.toMillis();
    this.rateLimitingEnabled = requireNonNull(produceRateLimitEnabledConfig);
    this.clock = requireNonNull(clock);
  }

  Optional<Duration> calculateGracePeriodExceeded(final long requestSize)
      throws RateLimitGracePeriodExceededException {
    if (!rateLimitingEnabled) {
      return Optional.empty();
    }
    long nowMs = clock.millis();

    TimeAndSize thisMessage = new TimeAndSize(nowMs, requestSize);
    addToRateLimiter(thisMessage);
    Optional<Duration> waitFor = getWaitFor(rateCounterSize.get(), byteCounterSize.get());

    if (!waitFor.isPresent()) {
      resetGracePeriodStart();
      return Optional.empty();
    }

    if (isOverGracePeriod(nowMs)) {
      throw new RateLimitGracePeriodExceededException(
          maxRequestsPerSecond, maxBytesPerSecond, Duration.ofMillis(gracePeriod));
    }
    return waitFor;
  }

  @VisibleForTesting
  void clear() {
    rateCounter.clear();
    rateCounterSize.set(0);
  }

  @VisibleForTesting
  void resetGracePeriodStart() {
    gracePeriodStart.set(-1);
  }

  private boolean isOverGracePeriod(Long nowMs) {
    if (gracePeriodStart.get() < 0 && gracePeriod != 0) {
      gracePeriodStart.set(nowMs);
      return false;
    }
    if (gracePeriod == 0 || gracePeriod < nowMs - gracePeriodStart.get()) {
      return true;
    }
    return false;
  }

  private void addToRateLimiter(TimeAndSize thisMessage) {
    rateCounter.add(thisMessage);
    rateCounterSize.incrementAndGet();
    byteCounterSize.addAndGet(thisMessage.size);

    synchronized (rateCounter) {
      if ((rateCounter.peekLast().time < thisMessage.time - ONE_SECOND_MS)) {
        rateCounter.clear();
        rateCounterSize.set(0);
      } else {
        while (rateCounter.peek().time < thisMessage.time - ONE_SECOND_MS) {
          TimeAndSize messageToRemove = rateCounter.poll();
          byteCounterSize.addAndGet(-messageToRemove.size);
          rateCounterSize.decrementAndGet();
        }
      }
    }
    return;
  }

  private Optional<Duration> getWaitFor(int currentCountRate, long currentByteRate) {
    if (currentCountRate <= maxRequestsPerSecond && currentByteRate <= maxBytesPerSecond) {
      return Optional.empty();
    }

    double waitForMs;
    if (currentCountRate > maxRequestsPerSecond) {
      waitForMs = ((double) currentCountRate / (double) maxRequestsPerSecond - 1) * 1000;
    } else {
      waitForMs = ((double) currentByteRate / (double) maxBytesPerSecond - 1) * 1000;
    }

    return Optional.of((Duration.ofMillis((long) waitForMs)));
  }

  private static final class TimeAndSize {
    private long time;
    private long size;

    TimeAndSize(long time, long size) {
      this.time = time;
      this.size = size;
    }
  }
}
