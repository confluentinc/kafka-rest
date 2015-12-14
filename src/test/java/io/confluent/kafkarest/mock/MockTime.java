/**
 * Copyright 2015 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/
package io.confluent.kafkarest.mock;

import java.util.concurrent.TimeUnit;

import io.confluent.kafkarest.Time;

public class MockTime implements Time {

  volatile long currentMs = 0;

  @Override
  public long milliseconds() {
    // Incrementing currentMs prevents tests from locking up
    // "waiting" for the time checks in the code.  Without this
    // the ConsumerManager never completed (it never calls sleep or waitOn)
    currentMs++;
    return currentMs;
  }

  @Override
  public long nanoseconds() {
    // No increment here.  If the code changes such that the above
    // millisecond method is never called will have to add an increment
    // here.
    return TimeUnit.NANOSECONDS.convert(currentMs, TimeUnit.MILLISECONDS);
  }

  @Override
  public void sleep(long ms) {
    currentMs += ms;
  }

  @Override
  public void waitOn(Object on, long ms) throws InterruptedException {
    // Introduced a thread.sleep to allow context switching in consumer worker.
    // Without this the worker appears to get stuck in a busy loop and actually
    // takes longer  to process the messages...
    Thread.sleep(1);
    currentMs += ms;
  }
}
