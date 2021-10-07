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

  private static ProduceRateLimitCounters produceRateLimitCounters = new ProduceRateLimitCounters();

  private ProduceRateLimitCounters() {}

  public static ProduceRateLimitCounters getProduceRateLimitCounters() {
    return produceRateLimitCounters;
  }

  final ConcurrentLinkedQueue<Long> rateCounter = new ConcurrentLinkedQueue<>();
  Optional<AtomicLong> gracePeriodStart = Optional.empty();

  public void clear() {
    rateCounter.clear();
  }

  public int size() {
    return rateCounter.size();
  }

  public Long peek() {
    return rateCounter.peek();
  }

  public Long poll() {
    return rateCounter.poll();
  }

  public void add(Long time) {
    rateCounter.add(time);
  }

  public void resetGracePeriodStart() {
    gracePeriodStart = Optional.empty();
  }

  public Optional<AtomicLong> get() {
    return gracePeriodStart;
  }

  public void setGracePeriodStart(final Optional<AtomicLong> gracePeriodStart) {
    this.gracePeriodStart = gracePeriodStart;
  }
}
