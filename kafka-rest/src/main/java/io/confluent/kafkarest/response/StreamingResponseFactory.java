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

package io.confluent.kafkarest.response;

import static java.util.Objects.requireNonNull;

import io.confluent.kafkarest.config.ConfigModule.StreamingMaxConnectionDurationConfig;
import io.confluent.kafkarest.config.ConfigModule.StreamingMaxConnectionGracePeriod;
import io.confluent.kafkarest.requests.JsonStreamIterable;
import io.confluent.kafkarest.resources.v3.V3ResourcesModule.ProduceScheduleCloseConnectionThreadPool;
import java.time.Duration;
import java.util.concurrent.ScheduledExecutorService;
import javax.inject.Inject;
import javax.ws.rs.container.AsyncResponse;

public final class StreamingResponseFactory {

  private final ChunkedOutputFactory chunkedOutputFactory;
  private final Duration maxDuration;
  private final Duration gracePeriod;
  private final ScheduledExecutorService executorService;

  @Inject
  public StreamingResponseFactory(
      ChunkedOutputFactory chunkedOutputFactory,
      @ProduceScheduleCloseConnectionThreadPool ScheduledExecutorService executorService,
      @StreamingMaxConnectionDurationConfig Duration maxDuration,
      @StreamingMaxConnectionGracePeriod Duration gracePeriod) {
    this.chunkedOutputFactory = requireNonNull(chunkedOutputFactory);
    this.executorService = requireNonNull(executorService);
    this.maxDuration = maxDuration;
    this.gracePeriod = gracePeriod;
  }

  public <I> StreamingResponse<I> compose(
      JsonStreamIterable<I> inputStream, AsyncResponse asyncResponse) {
    return StreamingResponse.compose(
        inputStream,
        asyncResponse,
        chunkedOutputFactory,
        maxDuration,
        gracePeriod,
        executorService);
  }

  public ResponseFlowableSubscriber createSubscriber(
      JsonStreamIterable inputStream, AsyncResponse asyncResponse) {
    return ResponseFlowableSubscriber.instance(
        inputStream,
        asyncResponse,
        chunkedOutputFactory,
        maxDuration,
        gracePeriod,
        executorService);
  }
}
