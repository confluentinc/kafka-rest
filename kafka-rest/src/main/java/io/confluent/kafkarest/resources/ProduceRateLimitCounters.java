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

package io.confluent.kafkarest.resources;

import java.util.Optional;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

public class ProduceRateLimitCounters {

  static final ConcurrentLinkedQueue<Long> rateCounter = new ConcurrentLinkedQueue<>();
  static Optional<AtomicLong> gracePeriodStart = Optional.empty();

  public static void clear() {
    rateCounter.clear();
  }

  public static int size() {
    return rateCounter.size();
  }

  public static Long peek() {
    return rateCounter.peek();
  }

  public static Long poll() {
    return rateCounter.poll();
  }

  public static void add(Long time) {
    rateCounter.add(time);
  }

  public static void resetGracePeriodStart() {
    gracePeriodStart = Optional.empty();
  }

  // This throws an exception if the gracePeriodStart is Optional.empty()
  public static Long get() {
    return gracePeriodStart.get().get();
  }

  public static void setGracePeriodStart(final Optional<AtomicLong> gracePeriodStart) {
    ProduceRateLimitCounters.gracePeriodStart = gracePeriodStart;
  }

  public static boolean gracePeriodPresent() {
    return gracePeriodStart.isPresent();
  }
}
