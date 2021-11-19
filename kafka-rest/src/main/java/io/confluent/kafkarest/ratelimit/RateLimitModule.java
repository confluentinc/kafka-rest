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

import javax.inject.Singleton;
import org.glassfish.hk2.utilities.binding.AbstractBinder;

/**
 * A module to configure bindings for {@link FixedCostRateLimiter} and {@link RequestRateLimiter}.
 */
final class RateLimitModule extends AbstractBinder {

  @Override
  protected void configure() {
    bindFactory(RequestRateLimiterFactory.class).to(RequestRateLimiter.class).in(Singleton.class);
  }
}
