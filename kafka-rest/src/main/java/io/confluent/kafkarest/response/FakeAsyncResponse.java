/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.kafkarest.response;

import java.util.Collection;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.TimeoutHandler;
import javax.ws.rs.core.Response;

/**
 * A fake {@link AsyncResponse} to be used in tests.
 */
public final class FakeAsyncResponse implements AsyncResponse {

  private enum State {
    SUSPENDED,
    CANCELLED,
    DONE
  }

  @GuardedBy("this")
  private State state = State.SUSPENDED;

  @GuardedBy("this")
  @Nullable
  private Object value;

  @GuardedBy("this")
  @Nullable
  private Throwable exception;

  @Nullable
  public Object getValue() {
    if (isSuspended()) {
      throw new IllegalStateException();
    }
    return value instanceof Response ? ((Response) value).getEntity() : value;
  }

  @Nullable
  public Throwable getException() {
    if (isSuspended()) {
      throw new IllegalStateException();
    }
    return exception;
  }

  @Override
  public synchronized boolean resume(Object response) {
    if (!isSuspended()) {
      return false;
    }
    state = State.DONE;
    value = response;
    return true;
  }

  @Override
  public synchronized boolean resume(Throwable response) {
    if (!isSuspended()) {
      return false;
    }
    state = State.DONE;
    exception = response;
    return true;
  }

  @Override
  public synchronized boolean cancel() {
    if (!isSuspended()) {
      return false;
    }
    state = State.CANCELLED;
    return true;
  }

  @Override
  public synchronized boolean cancel(int retryAfter) {
    throw new UnsupportedOperationException();
  }

  @Override
  public synchronized boolean cancel(Date retryAfter) {
    throw new UnsupportedOperationException();
  }

  @Override
  public synchronized boolean isSuspended() {
    return state == State.SUSPENDED;
  }

  @Override
  public synchronized boolean isCancelled() {
    return state == State.CANCELLED;
  }

  @Override
  public synchronized boolean isDone() {
    return state == State.DONE;
  }

  @Override
  public synchronized boolean setTimeout(long time, TimeUnit unit) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setTimeoutHandler(TimeoutHandler handler) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Collection<Class<?>> register(Class<?> callback) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Map<Class<?>, Collection<Class<?>>> register(Class<?> callback, Class<?>... callbacks) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Collection<Class<?>> register(Object callback) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Map<Class<?>, Collection<Class<?>>> register(Object callback, Object... callbacks) {
    throw new UnsupportedOperationException();
  }
}
