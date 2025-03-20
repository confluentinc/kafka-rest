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

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.LoadingCache;
import io.confluent.kafkarest.config.ConfigModule.RateLimitCostsConfig;
import io.confluent.kafkarest.config.ConfigModule.RateLimitDefaultCostConfig;
import io.confluent.kafkarest.config.ConfigModule.RateLimitPerClusterCacheExpiryConfig;
import io.confluent.kafkarest.extension.ResourceAccesslistFeature.ResourceName;
import io.confluent.kafkarest.ratelimit.RateLimitModule.RequestRateLimiterGeneric;
import io.confluent.kafkarest.ratelimit.RateLimitModule.RequestRateLimiterPerCluster;
import jakarta.inject.Inject;
import jakarta.inject.Provider;
import jakarta.ws.rs.container.DynamicFeature;
import jakarta.ws.rs.container.ResourceInfo;
import jakarta.ws.rs.core.FeatureContext;
import java.time.Duration;
import java.util.Map;

final class FixedCostRateLimitFeature implements DynamicFeature {
  private final Map<String, Integer> costs;
  private final int defaultCost;
  private final RequestRateLimiter genericRateLimiter;
  // DynamicFeature applies to every endpoint, therefore per cluster rate limit cache should be in
  // this level so that the cache is available for all endpoints
  private final LoadingCache<String, RequestRateLimiter> perClusterRateLimiterCache;

  @Inject
  FixedCostRateLimitFeature(
      @RateLimitCostsConfig Map<String, Integer> costs,
      @RateLimitDefaultCostConfig Integer defaultCost,
      @RateLimitPerClusterCacheExpiryConfig Duration rateLimitPerClusterCacheExpiryConfig,
      @RequestRateLimiterGeneric RequestRateLimiter genericRateLimiter,
      @RequestRateLimiterPerCluster Provider<RequestRateLimiter> perClusterRateLimiterProvider) {
    this.costs = requireNonNull(costs);
    this.defaultCost = defaultCost;
    this.genericRateLimiter = requireNonNull(genericRateLimiter);
    this.perClusterRateLimiterCache =
        CacheBuilder.newBuilder()
            .expireAfterAccess(rateLimitPerClusterCacheExpiryConfig)
            .build(new RequestRateLimiterCacheLoader(perClusterRateLimiterProvider));
  }

  @Override
  public void configure(ResourceInfo resourceInfo, FeatureContext context) {
    int cost = getCost(resourceInfo);
    if (cost == 0) {
      context.register(new NullContainerRequestFilter());
    } else {
      context.register(
          new FixedCostRateLimitRequestFilter(
              genericRateLimiter, cost, perClusterRateLimiterCache));
    }
  }

  /** Returns the cost of the resource/method for rate-limiting purposes. */
  private int getCost(ResourceInfo resourceInfo) {
    DoNotRateLimit methodIgnore =
        resourceInfo.getResourceMethod().getAnnotation(DoNotRateLimit.class);
    if (methodIgnore != null) {
      return 0;
    }
    DoNotRateLimit classIgnore =
        resourceInfo.getResourceClass().getAnnotation(DoNotRateLimit.class);
    if (classIgnore != null) {
      return 0;
    }
    ResourceName methodName = resourceInfo.getResourceMethod().getAnnotation(ResourceName.class);
    if (methodName != null && costs.containsKey(methodName.value())) {
      return costs.get(methodName.value());
    }
    ResourceName className = resourceInfo.getResourceClass().getAnnotation(ResourceName.class);
    if (className != null && costs.containsKey(className.value())) {
      return costs.get(className.value());
    }
    return defaultCost;
  }
}
