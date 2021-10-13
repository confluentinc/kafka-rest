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
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import javax.inject.Inject;

public final class RateLimiter {

  private static final int ONE_SECOND = 1000;
  private static final AtomicBoolean FALSE = new AtomicBoolean(false);

  private final int maxRequestsPerSecond;
  private final int gracePeriod;
  private final boolean rateLimitingEnabled;
  private final Time time;

  private final ConcurrentLinkedQueue<Long> rateCounter = new ConcurrentLinkedQueue<>();
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
    return calculateGracePeriodExceeded(FALSE);
  }

  public Optional<Duration> calculateGracePeriodExceeded(AtomicBoolean firstMessage)
      throws RateLimitGracePeriodExceededException {

    if (!rateLimitingEnabled) {
      return Optional.empty();
    }

    long now = time.milliseconds();
    int currentRate = firstMessage.getAndSet(false) ? rateCounter.size() : addAndGetRate(now);
    Optional<Duration> resumeAfterMs = getResumeAfterMs(currentRate);

    if (!resumeAfterMs.isPresent()) {
      resetGracePeriodStart();
      return Optional.empty();
    }

    if (overGracePeriod(now)) {
      throw new RateLimitGracePeriodExceededException(maxRequestsPerSecond, gracePeriod);
    }
    return resumeAfterMs;
  }

  public void clear() {
    rateCounter.clear();
  }

  public void resetGracePeriodStart() {
    gracePeriodStart.set(-1);
  }

  private boolean overGracePeriod(Long now) {
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
    while (rateCounter.peek() < now - ONE_SECOND) {
      rateCounter.poll();
    }
    return rateCounter.size();
  }

  private Optional<Duration> getResumeAfterMs(int currentRate) {
    if (currentRate <= maxRequestsPerSecond) {
      resetGracePeriodStart();
      return Optional.empty();
    }
    double resumeInMs = ((double) currentRate / (double) maxRequestsPerSecond - 1) * 1000;
    return Optional.of((Duration.ofMillis((long) resumeInMs)));
  }
}
