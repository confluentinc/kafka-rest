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

import java.time.Duration;
import org.glassfish.hk2.api.Factory;

/** A {@link Factory} for {@link RequestRateLimiter}. */
abstract class RequestRateLimiterFactory implements Factory<RequestRateLimiter> {
  private final RateLimitBackend backend;
  private final int permitsPerSecond;
  private final Duration timeout;

  RequestRateLimiterFactory(RateLimitBackend backend, Integer permitsPerSecond, Duration timeout) {
    this.backend = requireNonNull(backend);
    this.permitsPerSecond = permitsPerSecond;
    this.timeout = requireNonNull(timeout);
  }

  @Override
  public RequestRateLimiter provide() {
    switch (backend) {
      case GUAVA:
        return GuavaRateLimiter.create(permitsPerSecond, timeout);
      case RESILIENCE4J:
        return Resilience4JRateLimiter.create(permitsPerSecond, timeout);
      default:
        throw new AssertionError("Unknown enum constant: " + backend);
    }
  }

  @Override
  public void dispose(RequestRateLimiter rateLimiter) {}
}
