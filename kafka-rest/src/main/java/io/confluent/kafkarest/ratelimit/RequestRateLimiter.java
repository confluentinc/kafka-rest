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

/**
 * A server-wide request rate-limiter.
 *
 * <p>Requests are rate-limited based on the {@link
 * io.confluent.kafkarest.KafkaRestConfig#RATE_LIMIT_PERMITS_PER_SEC_CONFIG} and {@link
 * io.confluent.kafkarest.KafkaRestConfig#RATE_LIMIT_TIMEOUT_MS_CONFIG} configs.
 *
 * <p>This rate-limiter can be injected for manual rate-limiting. Be aware that, unless the
 * resource/method you're manually rate-limiting is marked with {@link DoNotRateLimit}, the request
 * will already have been automatically rate-limited.
 */
public abstract class RequestRateLimiter {

  RequestRateLimiter() {}

  /**
   * Ensures a rate of at most {@link
   * io.confluent.kafkarest.KafkaRestConfig#RATE_LIMIT_PERMITS_PER_SEC_CONFIG} requests passes
   * through.
   *
   * <p>Each {@code rateLimit()} call will wait up to {@link
   * io.confluent.kafkarest.KafkaRestConfig#RATE_LIMIT_TIMEOUT_MS_CONFIG} for permission to go
   * through.
   *
   * @param cost the cost of the request
   * @throws RateLimitExceededException if permission to go through has been denied
   */
  public abstract void rateLimit(int cost);
}
