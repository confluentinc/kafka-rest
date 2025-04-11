/*
 * Copyright 2022 Confluent Inc.
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

import io.confluent.kafkarest.config.ConfigModule.ProduceRateLimitCountGlobalConfig;
import io.confluent.kafkarest.config.ConfigModule.RateLimitTimeoutConfig;
import java.time.Duration;
import javax.inject.Inject;

public class RequestRateLimiterProduceCountGlobalFactory extends RequestRateLimiterFactory {

  @Inject
  public RequestRateLimiterProduceCountGlobalFactory(
      RateLimitBackend backend,
      @ProduceRateLimitCountGlobalConfig Integer permitsPerSecond,
      @RateLimitTimeoutConfig Duration timeout) {
    super(backend, permitsPerSecond, timeout);
  }
}
