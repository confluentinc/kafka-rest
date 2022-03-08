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

import static java.util.Objects.requireNonNull;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import io.confluent.kafkarest.config.ConfigModule.ProduceRateLimitCacheExpiryConfig;
import io.confluent.kafkarest.config.ConfigModule.ProduceRateLimitEnabledConfig;
import io.confluent.kafkarest.ratelimit.RateLimitModule.ProduceRateLimiterBytes;
import io.confluent.kafkarest.ratelimit.RateLimitModule.ProduceRateLimiterCount;
import io.confluent.kafkarest.ratelimit.RequestRateLimiter;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import javax.inject.Inject;
import javax.inject.Provider;

public class ProduceRateLimiters {

  private final boolean rateLimitingEnabled;
  private final LoadingCache<String, RequestRateLimiter> countCache;
  private final LoadingCache<String, RequestRateLimiter> bytesCache;
  private final Provider<RequestRateLimiter> countLimiterProvider;
  private final Provider<RequestRateLimiter> bytesLimiterProvider;
  private final Duration produceRateLimitCacheExpiryConfig;

  @Inject
  public ProduceRateLimiters(
      @ProduceRateLimiterCount Provider<RequestRateLimiter> countLimiterProvider,
      @ProduceRateLimiterBytes Provider<RequestRateLimiter> bytesLimiterProvider,
      @ProduceRateLimitEnabledConfig Boolean produceRateLimitEnabledConfig,
      @ProduceRateLimitCacheExpiryConfig Duration produceRateLimitCacheExpiryConfig) {
    this.rateLimitingEnabled = requireNonNull(produceRateLimitEnabledConfig);
    this.countLimiterProvider = requireNonNull(countLimiterProvider);
    this.bytesLimiterProvider = requireNonNull(bytesLimiterProvider);
    this.produceRateLimitCacheExpiryConfig = requireNonNull(produceRateLimitCacheExpiryConfig);

    countCache =
        CacheBuilder.newBuilder()
            .expireAfterAccess(produceRateLimitCacheExpiryConfig.toMillis(), TimeUnit.MILLISECONDS)
            .build(
                new CacheLoader<String, RequestRateLimiter>() {
                  @Override
                  public RequestRateLimiter load(String key) {
                    return countLimiterProvider.get();
                  }
                });

    bytesCache =
        CacheBuilder.newBuilder()
            .expireAfterAccess(produceRateLimitCacheExpiryConfig.toMillis(), TimeUnit.MILLISECONDS)
            .build(
                new CacheLoader<String, RequestRateLimiter>() {
                  @Override
                  public RequestRateLimiter load(String key) {
                    return bytesLimiterProvider.get();
                  }
                });
  }

  public void rateLimit(String clusterId, Optional<Long> requestSize) {
    if (!rateLimitingEnabled) {
      return;
    }

    RequestRateLimiter countRateLimiter = countCache.getUnchecked(clusterId);
    RequestRateLimiter byteRateLimiter = bytesCache.getUnchecked(clusterId);
    countRateLimiter.rateLimit(1);

    if (requestSize.isPresent()) {
      byteRateLimiter.rateLimit(requestSize.get().intValue());
    }
    // requestSize should always be present, but if there is no message size available
    // we allow the request through without limiting it
  }

  public void clear() {
    countCache.invalidateAll();
    bytesCache.invalidateAll();
  }
}
