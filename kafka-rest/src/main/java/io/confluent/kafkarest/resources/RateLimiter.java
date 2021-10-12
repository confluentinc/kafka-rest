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

import io.confluent.kafkarest.Time;
import io.confluent.kafkarest.config.ConfigModule.ProduceGracePeriodConfig;
import io.confluent.kafkarest.config.ConfigModule.ProduceRateLimitConfig;
import io.confluent.kafkarest.config.ConfigModule.ProduceRateLimitEnabledConfig;
import io.confluent.kafkarest.exceptions.RateLimitGracePeriodExceededException;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
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

  @Inject
  public RateLimiter(
      @ProduceGracePeriodConfig Integer produceGracePeriodConfig,
      @ProduceRateLimitConfig Integer produceRateLimitConfig,
      @ProduceRateLimitEnabledConfig Boolean produceRateLimitEnabledConfig) {
    this.maxRequestsPerSecond = produceRateLimitConfig;
    this.gracePeriod = produceGracePeriodConfig;
    this.rateLimitingEnabled = produceRateLimitEnabledConfig;
  }

  final ConcurrentLinkedQueue<Long> rateCounter = new ConcurrentLinkedQueue<>();
  Optional<AtomicLong> gracePeriodStart = Optional.empty();

  public void clear() {
    rateCounter.clear();
  }

  public void resetGracePeriodStart() {
    gracePeriodStart = Optional.empty();
  }

  public Optional<Long> getResumeAfterMs() {

    if (overRateLimit()) {
      double resumeInMs = ((double) rateCounter.size() / (double) maxRequestsPerSecond - 1) * 1000;
      return Optional.of((long) resumeInMs);
    } else {
      return Optional.empty();
    }
  }

  private boolean overRateLimit() {
    return rateCounter.size() > maxRequestsPerSecond;
  }

  private boolean overGracePeriod(Long now) {
    if (!gracePeriodStart.isPresent() && gracePeriod != 0) {
      gracePeriodStart = Optional.of(new AtomicLong(now));
      return false;
    } else if (gracePeriod == 0 || gracePeriod < now - gracePeriodStart.get().get()) {
      return true;
    }
    return false;
  }

  private void addToAndCullRateCounter(long now) {
    rateCounter.add(now);
    while (rateCounter.peek() < now - ONE_SECOND) {
      rateCounter.poll();
    }
    if (rateCounter.size() <= maxRequestsPerSecond) {
      resetGracePeriodStart();
    }
  }

  public boolean calculateGracePeriodExceeded(Time time) {
    return calculateGracePeriodExceeded(FALSE, time);
  }

  public boolean calculateGracePeriodExceeded(AtomicBoolean firstMessage, Time time) {
    if (rateLimitingEnabled) {
      if (!firstMessage.get()) {
        long now = time.milliseconds();
        addToAndCullRateCounter(now);
        if (overRateLimit()) {
          if (overGracePeriod(now)) {
            return true;
          }
        } else {
          resetGracePeriodStart();
        }
      } else {
        firstMessage.set(false);
      }
    }
    return false;
  }

  public CompletableFuture getLimitsFailureFuture() {
    CompletableFuture limitFuture = new CompletableFuture();
    limitFuture.completeExceptionally(
        new RateLimitGracePeriodExceededException(maxRequestsPerSecond, gracePeriod));
    return limitFuture;
  }
}
