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

import io.confluent.kafkarest.config.ConfigModule.RateLimitCostsConfig;
import io.confluent.kafkarest.config.ConfigModule.RateLimitDefaultCostConfig;
import io.confluent.kafkarest.extension.ResourceAccesslistFeature.ResourceName;
import io.confluent.kafkarest.ratelimit.RateLimitModule.RequestRateLimiterGeneric;
import java.util.Map;
import javax.inject.Inject;
import javax.ws.rs.container.DynamicFeature;
import javax.ws.rs.container.ResourceInfo;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.FeatureContext;

final class FixedCostRateLimitFeature implements DynamicFeature {
  private final Map<String, Integer> costs;
  private final int defaultCost;
  private final RequestRateLimiter requestRateLimiter;

  @Inject
  FixedCostRateLimitFeature(
      @RateLimitCostsConfig Map<String, Integer> costs,
      @RateLimitDefaultCostConfig Integer defaultCost,
      @Context @RequestRateLimiterGeneric RequestRateLimiter requestRateLimiter) {
    this.costs = requireNonNull(costs);
    this.defaultCost = defaultCost;
    this.requestRateLimiter = requireNonNull(requestRateLimiter);
  }

  @Override
  public void configure(ResourceInfo resourceInfo, FeatureContext context) {
    int cost = getCost(resourceInfo);
    if (cost == 0) {
      context.register(new FixedCostRateLimitRequestFilter(new NullFixedCostRateLimiter()));
    } else {
      context.register(
          new FixedCostRateLimitRequestFilter(
              new FixedCostRateLimiterImpl(requestRateLimiter, cost)));
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
