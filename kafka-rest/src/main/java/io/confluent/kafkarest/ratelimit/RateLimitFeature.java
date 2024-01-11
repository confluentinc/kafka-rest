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

import io.confluent.kafkarest.config.ConfigModule.RateLimitEnabledConfig;
import javax.inject.Inject;
import javax.ws.rs.core.Feature;
import javax.ws.rs.core.FeatureContext;
import org.glassfish.hk2.utilities.binding.AbstractBinder;

/**
 * A feature that configures request rate-limiting in the server.
 *
 * <p>This feature will configure a server-wide {@link RequestRateLimiter} based on the {@link
 * io.confluent.kafkarest.KafkaRestConfig#RATE_LIMIT_BACKEND_CONFIG}, {@link
 * io.confluent.kafkarest.KafkaRestConfig#RATE_LIMIT_PERMITS_PER_SEC_CONFIG} and {@link
 * io.confluent.kafkarest.KafkaRestConfig#RATE_LIMIT_TIMEOUT_MS_CONFIG} configs. This rate-limiter
 * can be used for manually rate-limiting requests.
 *
 * <p>In addition to that, this feature also configures a per-endpoint filter that automatically
 * rate-limits requests based on the {@link
 * io.confluent.kafkarest.KafkaRestConfig#RATE_LIMIT_COSTS_CONFIG} and {@link
 * io.confluent.kafkarest.KafkaRestConfig#RATE_LIMIT_DEFAULT_COST_CONFIG} configs.
 *
 * <p>This feature can be enabled/disabled via the {@link
 * io.confluent.kafkarest.KafkaRestConfig#RATE_LIMIT_ENABLE_CONFIG} config.
 */
public final class RateLimitFeature implements Feature {
  private final boolean rateLimitEnabled;

  @Inject
  RateLimitFeature(@RateLimitEnabledConfig Boolean rateLimitEnabled) {
    this.rateLimitEnabled = rateLimitEnabled;
  }

  @Override
  public boolean configure(FeatureContext context) {
    if (rateLimitEnabled) {
      context.register(RateLimitModule.class);
      context.register(FixedCostRateLimitFeature.class);
      return true;
    } else {
      context.register(NullRateLimitModule.class);
      return false;
    }
  }

  /**
   * A module to make sure {@link RequestRateLimiter} is injectable, even if rate-limiting is
   * disabled.
   */
  private static final class NullRateLimitModule extends AbstractBinder {

    @Override
    protected void configure() {
      bind(NullRequestRateLimiter.class).to(RequestRateLimiter.class);
    }
  }
}
