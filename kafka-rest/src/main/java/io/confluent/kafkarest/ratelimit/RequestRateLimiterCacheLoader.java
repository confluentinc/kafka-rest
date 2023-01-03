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

import com.google.common.cache.CacheLoader;
import javax.inject.Provider;

/**
 * RequestRateLimiterCacheLoader is a {@link CacheLoader} to help create a {@link
 * RequestRateLimiter} if it does not exist in the cache
 */
public final class RequestRateLimiterCacheLoader extends CacheLoader<String, RequestRateLimiter> {

  private final Provider<RequestRateLimiter> rateLimiter;

  public RequestRateLimiterCacheLoader(Provider<RequestRateLimiter> rateLimiter) {
    this.rateLimiter = requireNonNull(rateLimiter);
  }

  @Override
  public RequestRateLimiter load(String key) {
    return rateLimiter.get();
  }
}
