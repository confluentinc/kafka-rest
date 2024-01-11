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

import java.time.Duration;
import org.junit.jupiter.api.Disabled;

// TODO ddimitrov This continues being way too flaky.
//  Until we fix it (KREST-3850), we should ignore it, as it might be hiding even worse errors.
@Disabled
public final class GuavaRateLimitTest extends AbstractRateLimitEnabledTest {

  @Override
  RateLimitBackend getBackend() {
    return RateLimitBackend.GUAVA;
  }

  @Override
  Duration getRate() {
    return Duration.ofMillis(1);
  }

  @Override
  int getWarmupRequests() {
    return 500;
  }

  @Override
  int getTotalRequests() {
    return 1500;
  }

  @Override
  int getSlack() {
    return 10;
  }
}
