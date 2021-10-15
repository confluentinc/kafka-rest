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

package io.confluent.kafkarest.resources;

import static java.util.Objects.requireNonNull;

import io.confluent.kafkarest.Time;
import io.confluent.kafkarest.config.ConfigModule.ProduceGracePeriodConfig;
import io.confluent.kafkarest.config.ConfigModule.ProduceRateLimitConfig;
import io.confluent.kafkarest.config.ConfigModule.ProduceRateLimitEnabledConfig;
import io.confluent.kafkarest.exceptions.RateLimitGracePeriodExceededException;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import javax.inject.Inject;

public final class RateLimiter {

  private static final int ONE_SECOND_MS = 1000;

  private final int maxRequestsPerSecond;
  private final int gracePeriod;
  private final boolean rateLimitingEnabled;
  private final Time time;
  private final AtomicInteger rateCounterSize = new AtomicInteger(0);

  private final ConcurrentLinkedDeque<Long> rateCounter = new ConcurrentLinkedDeque<>();
  private final AtomicLong gracePeriodStart = new AtomicLong(-1);

  @Inject
  public RateLimiter(
      @ProduceGracePeriodConfig Integer produceGracePeriodConfig,
      @ProduceRateLimitConfig Integer produceRateLimitConfig,
      @ProduceRateLimitEnabledConfig Boolean produceRateLimitEnabledConfig,
      Time time) {
    this.maxRequestsPerSecond = requireNonNull(produceRateLimitConfig);
    this.gracePeriod = requireNonNull(produceGracePeriodConfig);
    this.rateLimitingEnabled = requireNonNull(produceRateLimitEnabledConfig);
    this.time = requireNonNull(time);
  }

  public Optional<Duration> calculateGracePeriodExceeded()
      throws RateLimitGracePeriodExceededException {
    if (!rateLimitingEnabled) {
      return Optional.empty();
    }

    long now = time.milliseconds();
    int currentRate = addAndGetRate(now);
    Optional<Duration> resumeAfter = getResumeAfter(currentRate);

    if (!resumeAfter.isPresent()) {
      resetGracePeriodStart();
      return Optional.empty();
    }

    if (isOverGracePeriod(now)) {
      throw new RateLimitGracePeriodExceededException(maxRequestsPerSecond, gracePeriod);
    }
    return resumeAfter;
  }

  public void clear() {
    rateCounter.clear();
    rateCounterSize.set(0);
  }

  public void resetGracePeriodStart() {
    gracePeriodStart.set(-1);
  }

  private boolean isOverGracePeriod(Long now) {
    if (gracePeriodStart.get() < 0 && gracePeriod != 0) {
      gracePeriodStart.set(now);
      return false;
    }
    if (gracePeriod == 0 || gracePeriod < now - gracePeriodStart.get()) {
      return true;
    }
    return false;
  }

  private int addAndGetRate(long now) {
    rateCounter.add(now);
    rateCounterSize.incrementAndGet();

    synchronized (rateCounter) {
      if (rateCounter.peekLast() < now - ONE_SECOND_MS) {
        rateCounter.clear();
        rateCounterSize.set(0);
      } else {
        while (rateCounter.peek() < now - ONE_SECOND_MS) {
          rateCounter.poll();
          rateCounterSize.decrementAndGet();
        }
      }
    }
    return rateCounterSize.get();
  }

  private Optional<Duration> getResumeAfter(int currentRate) {
    if (currentRate <= maxRequestsPerSecond) {
      return Optional.empty();
    }
    double resumeInMs = ((double) currentRate / (double) maxRequestsPerSecond - 1) * 1000;
    return Optional.of((Duration.ofMillis((long) resumeInMs)));
  }
}
