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

package io.confluent.kafkarest.exceptions;

import java.util.concurrent.CompletionException;

/**
 * A {@link CompletionException} which doesn't fill its stack trace. Can be explicitly used instead
 * of the regular one in performance-sensitive code paths where we throw during async processing
 * with {@link java.util.concurrent.CompletableFuture}s.
 *
 * <ul>
 *   <li>One good example of that is Produce v3 rate-limiting.
 * </ul>
 */
public final class StacklessCompletionException extends CompletionException {

  public StacklessCompletionException(Throwable cause) {
    super(cause);
  }

  @SuppressWarnings("unused")
  public StacklessCompletionException(String message, Throwable cause) {
    super(message, cause);
  }

  @Override
  public synchronized Throwable fillInStackTrace() {
    return this;
  }
}
