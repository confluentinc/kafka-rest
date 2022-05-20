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

package io.confluent.kafkarest.ratelimit;

import static java.util.Objects.requireNonNull;

import io.github.resilience4j.ratelimiter.RateLimiter;
import io.github.resilience4j.ratelimiter.RateLimiterConfig;
import java.time.Duration;

/** A {@link RequestRateLimiter} implementation based on Resilience4j {@link RateLimiter}. */
final class Resilience4JRateLimiter extends RequestRateLimiter {
  private final RateLimiter delegate;

  private Resilience4JRateLimiter(RateLimiter delegate) {
    this.delegate = requireNonNull(delegate);
  }

  static Resilience4JRateLimiter create(int permitsPerSecond, Duration timeout) {
    RateLimiterConfig config =
        RateLimiterConfig.custom()
            .timeoutDuration(timeout)
            .limitRefreshPeriod(Duration.ofSeconds(1))
            .limitForPeriod(permitsPerSecond)
            .build();
    return new Resilience4JRateLimiter(RateLimiter.of("Resilience4JRateLimiter", config));
  }

  @Override
  public void rateLimit(int cost) {
    if (!delegate.acquirePermission(cost)) {
      throw new RateLimitExceededException();
    }
  }
}
