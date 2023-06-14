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

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

import com.google.common.cache.LoadingCache;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;

/**
 * A {@link ContainerRequestFilter} that automatically applies a request rate-limit at a fixed cost
 * based on the resource/method being requested per cluster, according to the {@link
 * io.confluent.kafkarest.KafkaRestConfig#RATE_LIMIT_COSTS_CONFIG} and {@link
 * io.confluent.kafkarest.KafkaRestConfig#RATE_LIMIT_DEFAULT_COST_CONFIG} configs.
 */
final class FixedCostRateLimitRequestFilter implements ContainerRequestFilter {
  private final RequestRateLimiter genericRateLimiter;
  private final int cost;
  private final LoadingCache<String, RequestRateLimiter> perClusterRateLimiterCache;

  FixedCostRateLimitRequestFilter(
      RequestRateLimiter genericRateLimiter,
      int cost,
      LoadingCache<String, RequestRateLimiter> perClusterRateLimiterCache) {
    checkArgument(cost > 0, "Cost must be positive");
    this.genericRateLimiter = requireNonNull(genericRateLimiter);
    this.cost = cost;
    this.perClusterRateLimiterCache = requireNonNull(perClusterRateLimiterCache);
  }

  @Override
  public void filter(ContainerRequestContext requestContext) {
    // apply per cluster rate limiter
    String clusterId = requestContext.getUriInfo().getPathParameters(true).getFirst("clusterId");
    if (clusterId != null) {
      RequestRateLimiter rateLimiter = perClusterRateLimiterCache.getUnchecked(clusterId);
      rateLimiter.rateLimit(cost);
    }
    try {
      // apply generic (global) rate limiter
      genericRateLimiter.rateLimit(cost);
    } catch (RateLimitExceededException e) {
      throw new RateLimitExceededException(42907);
    }
  }
}
