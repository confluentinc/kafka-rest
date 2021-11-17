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
import io.confluent.kafkarest.config.ConfigModule.ProduceRateLimitConfig;
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
  private final long gracePeriod;
  private final boolean rateLimitingEnabled;
  private final Clock clock;
  private final AtomicInteger rateCounterSize = new AtomicInteger(0);

  private final ConcurrentLinkedDeque<Long> rateCounter = new ConcurrentLinkedDeque<>();
  private final AtomicLong gracePeriodStart = new AtomicLong(-1);

  @Inject
  ProduceRateLimiter(
      @ProduceGracePeriodConfig Duration produceGracePeriodConfig,
      @ProduceRateLimitConfig Integer produceRateLimitConfig,
      @ProduceRateLimitEnabledConfig Boolean produceRateLimitEnabledConfig,
      Clock clock) {
    this.maxRequestsPerSecond = requireNonNull(produceRateLimitConfig);
    this.gracePeriod = produceGracePeriodConfig.toMillis();
    this.rateLimitingEnabled = requireNonNull(produceRateLimitEnabledConfig);
    this.clock = requireNonNull(clock);
  }

  Optional<Duration> calculateGracePeriodExceeded() throws RateLimitGracePeriodExceededException {
    if (!rateLimitingEnabled) {
      return Optional.empty();
    }

    long nowMs = clock.millis();
    int currentRate = addAndGetRate(nowMs);
    Optional<Duration> waitFor = getWaitFor(currentRate);

    if (!waitFor.isPresent()) {
      resetGracePeriodStart();
      return Optional.empty();
    }

    if (isOverGracePeriod(nowMs)) {
      throw new RateLimitGracePeriodExceededException(
          maxRequestsPerSecond, Duration.ofMillis(gracePeriod));
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

  private int addAndGetRate(long nowMs) {
    rateCounter.add(nowMs);
    rateCounterSize.incrementAndGet();

    synchronized (rateCounter) {
      if (rateCounter.peekLast() < nowMs - ONE_SECOND_MS) {
        rateCounter.clear();
        rateCounterSize.set(0);
      } else {
        while (rateCounter.peek() < nowMs - ONE_SECOND_MS) {
          rateCounter.poll();
          rateCounterSize.decrementAndGet();
        }
      }
    }
    return rateCounterSize.get();
  }

  private Optional<Duration> getWaitFor(int currentRate) {
    if (currentRate <= maxRequestsPerSecond) {
      return Optional.empty();
    }
    double waitForMs = ((double) currentRate / (double) maxRequestsPerSecond - 1) * 1000;
    return Optional.of((Duration.ofMillis((long) waitForMs)));
  }
}
