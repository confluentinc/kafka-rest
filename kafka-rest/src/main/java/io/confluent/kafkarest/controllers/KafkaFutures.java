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

package io.confluent.kafkarest.controllers;

import java.util.concurrent.CompletableFuture;
import org.apache.kafka.common.KafkaFuture;

final class KafkaFutures {

  private KafkaFutures() {
  }

  static <T> CompletableFuture<T> toCompletableFuture(KafkaFuture<T> kafkaFuture) {
    CompletableFuture<T> completableFuture = new CompletableFuture<>();
    kafkaFuture.whenComplete(
        (value, exception) -> {
          if (exception == null) {
            completableFuture.complete(value);
          } else {
            completableFuture.completeExceptionally(exception);
          }
        });
    return completableFuture;
  }
}
