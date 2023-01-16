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

package io.confluent.kafkarest.common;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Function;
import java.util.stream.Collectors;

public final class CompletableFutures {

  private CompletableFutures() {
  }

  /**
   * Returns a {@link CompletableFuture} which will complete after all {@code futures} have
   * completed, and when complete, will contain the value of all {@code futures}.
   *
   * <p>If any of the {@code futures} fail, the resulting {@code CompletableFuture} will complete
   * exceptionally with that future's failure.
   */
  public static <T> CompletableFuture<List<T>> allAsList(List<CompletableFuture<T>> futures) {
    return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
        .thenApply(
            none -> futures.stream().map(CompletableFuture::join).collect(Collectors.toList()));
  }

  /**
   * Returns a {@link CompletableFuture} that is completed exceptionally with the given {@code
   * exception}.
   */
  public static <T> CompletableFuture<T> failedFuture(Throwable exception) {
    CompletableFuture<T> future = new CompletableFuture<>();
    future.completeExceptionally(exception);
    return future;
  }

  /**
   * Returns a new {@link CompletableFuture} that is completed when {@code future} is complete,
   * catching the given {@link Throwable exceptionClass}.
   *
   * <p>If {@code future} completes normally, then the returned future completes normally with the
   * same value. If {@code future} completes exceptionally with an {@code exceptionClass}, the
   * returned future completes with the result of the {@code handler}. Otherwise, the returned
   * future will complete exceptionally with the same exception as {@code future}.
   */
  public static <T, E extends Throwable> CompletableFuture<T> catchingCompose(
      CompletableFuture<T> future,
      Class<E> exceptionClass,
      Function<? super E, ? extends CompletableFuture<T>> handler) {
    return future.handle(
        (value, error) -> {
          if (error == null) {
            return CompletableFuture.completedFuture(value);
          } else if (exceptionClass.isInstance(error.getCause())) {
            return handler.apply(exceptionClass.cast(error.getCause()));
          } else if (error instanceof CompletionException) {
            throw (CompletionException) error;
          } else {
            throw new AssertionError(error); // If this happens, CompletableFuture is broken.
          }
        })
        .thenCompose(wrapped -> wrapped);
  }
}
