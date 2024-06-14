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

package io.confluent.kafkarest;

import java.util.function.Supplier;

public class SystemTime implements Time {
  private final org.apache.kafka.common.utils.Time delegate = Time.SYSTEM;

  @Override
  public long milliseconds() {
    return delegate.milliseconds();
  }

  @Override
  public long nanoseconds() {
    return delegate.nanoseconds();
  }

  @Override
  public void sleep(long ms) {
    delegate.sleep(ms);
  }

  @Override
  public void waitObject(Object o, Supplier<Boolean> supplier, long l) throws InterruptedException {
    delegate.waitObject(o, supplier, l);
  }

  @Override
  public void waitOn(Object on, long ms) throws InterruptedException {
    on.wait(ms);
  }
}
