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
import io.confluent.kafkarest.config.ConfigModule.ProduceGracePeriodConfig;
import io.confluent.kafkarest.config.ConfigModule.ProduceRateLimitBytesConfig;
import io.confluent.kafkarest.config.ConfigModule.ProduceRateLimitCacheExpiryConfig;
import io.confluent.kafkarest.config.ConfigModule.ProduceRateLimitCountConfig;
import io.confluent.kafkarest.config.ConfigModule.ProduceRateLimitEnabledConfig;
import java.time.Clock;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import javax.inject.Inject;

public class ProduceRateLimiters {

  private final int maxRequestsPerSecond;
  private final int maxBytesPerSecond;
  private final Duration gracePeriod;
  private final boolean rateLimitingEnabled;
  private final LoadingCache<String, ProduceRateLimiter> cache;

  @Inject
  public ProduceRateLimiters(
      @ProduceGracePeriodConfig Duration produceGracePeriodConfig,
      @ProduceRateLimitCountConfig Integer produceRateLimitCountConfig,
      @ProduceRateLimitBytesConfig Integer produceRateLimitBytesConfig,
      @ProduceRateLimitEnabledConfig Boolean produceRateLimitEnabledConfig,
      @ProduceRateLimitCacheExpiryConfig Duration produceRateLimitCacheExpiryConfig,
      Clock time) {
    this.maxRequestsPerSecond = requireNonNull(produceRateLimitCountConfig);
    this.gracePeriod = requireNonNull(produceGracePeriodConfig);
    this.rateLimitingEnabled = requireNonNull(produceRateLimitEnabledConfig);
    this.maxBytesPerSecond = requireNonNull(produceRateLimitBytesConfig);
    requireNonNull(time);

    cache =
        CacheBuilder.newBuilder()
            .expireAfterAccess(produceRateLimitCacheExpiryConfig.toMillis(), TimeUnit.MILLISECONDS)
            .build(
                new CacheLoader<String, ProduceRateLimiter>() {
                  @Override
                  public ProduceRateLimiter load(String key) {
                    return new ProduceRateLimiter(
                        gracePeriod,
                        maxRequestsPerSecond,
                        maxBytesPerSecond,
                        produceRateLimitEnabledConfig,
                        time);
                  }
                });
  }

  public Optional<Duration> calculateGracePeriodExceeded(String clusterId, long requestSize) {
    if (!rateLimitingEnabled) {
      return Optional.empty();
    }
    ProduceRateLimiter rateLimiter = cache.getUnchecked(clusterId);
    Optional<Duration> waitTime = rateLimiter.calculateGracePeriodExceeded(requestSize);
    return waitTime;
  }

  public void clear() {
    cache.invalidateAll();
  }
}
