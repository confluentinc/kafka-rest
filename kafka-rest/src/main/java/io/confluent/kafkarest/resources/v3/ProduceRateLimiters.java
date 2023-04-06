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

package io.confluent.kafkarest.resources.v3;

import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import io.confluent.kafkarest.config.ConfigModule.ProduceRateLimitCacheExpiryConfig;
import io.confluent.kafkarest.config.ConfigModule.ProduceRateLimitEnabledConfig;
import io.confluent.kafkarest.ratelimit.RateLimitModule.ProduceRateLimiterBytes;
import io.confluent.kafkarest.ratelimit.RateLimitModule.ProduceRateLimiterBytesGlobal;
import io.confluent.kafkarest.ratelimit.RateLimitModule.ProduceRateLimiterCount;
import io.confluent.kafkarest.ratelimit.RateLimitModule.ProduceRateLimiterCountGlobal;
import io.confluent.kafkarest.ratelimit.RequestRateLimiter;
import java.time.Duration;
import javax.inject.Inject;
import javax.inject.Provider;

public class ProduceRateLimiters {

  private final boolean rateLimitingEnabled;
  private final LoadingCache<String, RequestRateLimiter> countCache;
  private final LoadingCache<String, RequestRateLimiter> bytesCache;
  private final Provider<RequestRateLimiter> bytesLimiterGlobal;
  private final Provider<RequestRateLimiter> countLimiterGlobal;

  @Inject
  public ProduceRateLimiters(
      @ProduceRateLimiterCount Provider<RequestRateLimiter> countLimiterProvider,
      @ProduceRateLimiterBytes Provider<RequestRateLimiter> bytesLimiterProvider,
      @ProduceRateLimiterCountGlobal Provider<RequestRateLimiter> countLimiterGlobal,
      @ProduceRateLimiterBytesGlobal Provider<RequestRateLimiter> bytesLimiterGlobal,
      @ProduceRateLimitEnabledConfig Boolean produceRateLimitEnabledConfig,
      @ProduceRateLimitCacheExpiryConfig Duration produceRateLimitCacheExpiryConfig) {
    this.rateLimitingEnabled = requireNonNull(produceRateLimitEnabledConfig);
    this.countLimiterGlobal = requireNonNull(countLimiterGlobal);
    this.bytesLimiterGlobal = requireNonNull(bytesLimiterGlobal);

    countCache =
        CacheBuilder.newBuilder()
            .expireAfterAccess(produceRateLimitCacheExpiryConfig)
            .build(new RequestRateLimiterCacheLoader(countLimiterProvider));
    bytesCache =
        CacheBuilder.newBuilder()
            .expireAfterAccess(produceRateLimitCacheExpiryConfig)
            .build(new RequestRateLimiterCacheLoader(bytesLimiterProvider));
  }

  public void rateLimit(String clusterId, long requestSize) {
    if (!rateLimitingEnabled) {
      return;
    }
    // Global rate limit first to reduce CPU usage under load
    // https://confluentinc.atlassian.net/browse/KREST-4979
    countLimiterGlobal.get().rateLimit(1);
    bytesLimiterGlobal.get().rateLimit(toIntExact(requestSize));
    RequestRateLimiter countRateLimiter = countCache.getUnchecked(clusterId);
    RequestRateLimiter byteRateLimiter = bytesCache.getUnchecked(clusterId);
    countRateLimiter.rateLimit(1);
    byteRateLimiter.rateLimit(toIntExact(requestSize));
  }

  public void clear() {
    countCache.invalidateAll();
    bytesCache.invalidateAll();
  }

  private static final class RequestRateLimiterCacheLoader
      extends CacheLoader<String, RequestRateLimiter> {
    private final Provider<RequestRateLimiter> rateLimiter;

    private RequestRateLimiterCacheLoader(Provider<RequestRateLimiter> rateLimiter) {
      this.rateLimiter = requireNonNull(rateLimiter);
    }

    @Override
    public RequestRateLimiter load(String key) {
      return rateLimiter.get();
    }
  }
}
