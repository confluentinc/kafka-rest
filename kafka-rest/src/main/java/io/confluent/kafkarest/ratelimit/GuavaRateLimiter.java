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

import com.google.common.util.concurrent.RateLimiter;
import java.time.Duration;

/** A {@link RequestRateLimiter} implementation based on Guava {@link RateLimiter}. */
final class GuavaRateLimiter extends RequestRateLimiter {
  private final RateLimiter delegate;
  private final Duration timeout;

  private GuavaRateLimiter(RateLimiter delegate, Duration timeout) {
    this.delegate = requireNonNull(delegate);
    this.timeout = requireNonNull(timeout);
  }

  static GuavaRateLimiter create(int permitsPerSecond, Duration timeout) {
    return new GuavaRateLimiter(RateLimiter.create(permitsPerSecond), timeout);
  }

  @Override
  public void rateLimit(int cost) {
    if (!delegate.tryAcquire(cost, timeout)) {
      throw new RateLimitExceededException();
    }
  }
}
