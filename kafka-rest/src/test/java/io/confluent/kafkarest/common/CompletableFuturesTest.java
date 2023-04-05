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

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class CompletableFuturesTest {

  @Test
  public void failingStage_catchingFailure_catchesFailure() throws Exception {
    CompletableFuture<String> future = new CompletableFuture<>();
    CompletableFuture<Integer> throwing =
        CompletableFutures.catchingCompose(
            future.thenApply(str -> str.length() + Integer.parseInt(str)),
            NumberFormatException.class,
            error ->  CompletableFuture.completedFuture(0));
    future.complete("foobar"); // Integer.parseInt(str) will throw NumberFormatException

    assertEquals(0, (int) throwing.get());
  }

  @Test
  public void failingStage_catchingDifferentException_propagatesFailure() {
    CompletableFuture<String> future = new CompletableFuture<>();
    CompletableFuture<Integer> throwing =
        CompletableFutures.catchingCompose(
            future.thenApply(str -> str.length() + Integer.parseInt(str)),
            NumberFormatException.class,
            error ->  CompletableFuture.completedFuture(0));
    future.complete(null); // str.length() will throw NullPointerException

    try {
      throwing.join();
    } catch (CompletionException e) {
      assertEquals(NullPointerException.class, e.getCause().getClass());
    }
  }

  @Test
  public void failingStage_multiCatch_correctHandlerTriggered() throws Exception {
    CompletableFuture<String> future = new CompletableFuture<>();
    CompletableFuture<Integer> throwing =
        CompletableFutures.catchingCompose(
            CompletableFutures.catchingCompose(
                future.thenApply(str -> str.length() + Integer.parseInt(str)),
                NumberFormatException.class,
                error ->  CompletableFuture.completedFuture(0)),
            NullPointerException.class,
            error -> CompletableFuture.completedFuture(1));
    future.complete("foobar"); // Integer.parseInt(str) will throw NumberFormatException

    assertEquals(0, (int) throwing.get());
  }

  @Test
  public void completedStage_catchingFailure_propagatesResult() throws Exception {
    CompletableFuture<String> future = new CompletableFuture<>();
    CompletableFuture<Integer> throwing =
        CompletableFutures.catchingCompose(
            future.thenApply(str -> str.length() + Integer.parseInt(str)),
            NumberFormatException.class,
            error ->  CompletableFuture.completedFuture(0));
    future.complete("100"); // 3 + 100 = 103

    assertEquals(103, (int) throwing.get());
  }

  @Test
  public void failingStage_checkedException_catchingFailure_catchesFailure() throws Exception {
    CompletableFuture<String> future = new CompletableFuture<>();
    CompletableFuture<Integer> throwing =
        CompletableFutures.catchingCompose(
            future.thenApply(str -> str.length() + Integer.parseInt(str)),
            IOException.class,
            error ->  CompletableFuture.completedFuture(0));
    future.completeExceptionally(new IOException());

    assertEquals(0, (int) throwing.get());
  }

  @Test
  public void failingStage_checkedException_catchingDifferentException_propagatesFailure() {
    CompletableFuture<String> future = new CompletableFuture<>();
    CompletableFuture<Integer> throwing =
        CompletableFutures.catchingCompose(
            future.thenApply(str -> str.length() + Integer.parseInt(str)),
            NumberFormatException.class,
            error ->  CompletableFuture.completedFuture(0));
    future.completeExceptionally(new IOException());

    try {
      throwing.join();
    } catch (CompletionException e) {
      assertEquals(IOException.class, e.getCause().getClass());
    }
  }

  @Test
  public void failingStage_checkedException_multiCatch_correctHandlerTriggered() throws Exception {
    CompletableFuture<String> future = new CompletableFuture<>();
    CompletableFuture<Integer> throwing =
        CompletableFutures.catchingCompose(
            CompletableFutures.catchingCompose(
                CompletableFutures.catchingCompose(
                    future.thenApply(str -> str.length() + Integer.parseInt(str)),
                    NumberFormatException.class,
                    error ->  CompletableFuture.completedFuture(0)),
                NullPointerException.class,
                error -> CompletableFuture.completedFuture(1)),
            IOException.class,
            error -> CompletableFuture.completedFuture(3));
    future.completeExceptionally(new IOException());

    assertEquals(3, (int) throwing.get());
  }
}
