/*
 * Copyright 2018 Confluent Inc.
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
package io.confluent.kafkarest.mock;

import java.util.concurrent.TimeUnit;

import io.confluent.kafkarest.Time;
import java.util.function.Supplier;

public class MockTime implements Time {

  volatile long currentMs = 0;

  @Override
  public long milliseconds() {
    return currentMs;
  }

  @Override
  public long nanoseconds() {
    return TimeUnit.NANOSECONDS.convert(currentMs, TimeUnit.MILLISECONDS);
  }

  @Override
  public void sleep(long ms) {
    currentMs += ms;
  }

  @Override
  public void waitObject(Object o, Supplier<Boolean> supplier, long l) throws InterruptedException {

  }

  @Override
  public void waitOn(Object on, long ms) throws InterruptedException {
    synchronized (on) {
      on.wait(ms);
    }
    currentMs += ms;
  }
}
