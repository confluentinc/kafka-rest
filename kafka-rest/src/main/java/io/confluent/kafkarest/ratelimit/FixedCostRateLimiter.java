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
 * A per-resource fixed-cost rate-limiter.
 *
 * <p>In a request scope, the cost used for each {@code rateLimit()} call is determined from the
 * resource/method being requested, based on the {@link
 * io.confluent.kafkarest.KafkaRestConfig#RATE_LIMIT_COSTS_CONFIG}. If no cost has been configured
 * for the particular resource/method being requested, {@link
 * io.confluent.kafkarest.KafkaRestConfig#RATE_LIMIT_DEFAULT_COST_CONFIG} is used instead. A
 * configured cost of zero means the request is not rate-limited.
 *
 * @see RequestRateLimiter
 */
interface FixedCostRateLimiter {

  /**
   * Ensures a rate of at most {@link
   * io.confluent.kafkarest.KafkaRestConfig#RATE_LIMIT_PERMITS_PER_SEC_CONFIG} requests passes
   * through.
   *
   * <p>Each {@code rateLimit()} call will wait up to {@link
   * io.confluent.kafkarest.KafkaRestConfig#RATE_LIMIT_TIMEOUT_MS_CONFIG} for permission to go
   * through.
   *
   * @throws RateLimitExceededException if permission to go through has been denied
   * @see RequestRateLimiter#rateLimit(int)
   */
  void rateLimit();
}
