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

import static io.confluent.kafkarest.response.CompositeErrorMapper.EXCEPTION_MAPPER;

import io.confluent.kafkarest.exceptions.StatusCodeException;
import io.confluent.kafkarest.requests.JsonStreamIterable;
import io.confluent.kafkarest.response.StreamingResponse.ResultOrError;
import io.reactivex.rxjava3.annotations.NonNull;
import io.reactivex.rxjava3.core.FlowableSubscriber;
import java.time.Duration;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import org.glassfish.jersey.server.ChunkedOutput;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ResponseFlowableSubscriber implements FlowableSubscriber<ResultOrError> {
  private static final Logger log = LoggerFactory.getLogger(ResponseFlowableSubscriber.class);

  private final Duration maxDuration;
  private final Duration gracePeriod;

  private final ChunkedOutput<ResultOrError> sink;
  private final JsonStreamIterable source;

  private Subscription subscription;

  public ResponseFlowableSubscriber(
      JsonStreamIterable inputStream,
      AsyncResponse asyncResponse,
      ChunkedOutputFactory chunkedOutputFactory,
      Duration maxDuration,
      Duration gracePeriod,
      ScheduledExecutorService executorService) {
    this.maxDuration = maxDuration;
    this.gracePeriod = gracePeriod;
    this.source = inputStream;
    this.sink = chunkedOutputFactory.getChunkedOutput();
    asyncResponse.resume(Response.ok(this.sink).build());
    scheduleConnectionCloseOnOverMaxDuration(executorService);
  }

  public static ResponseFlowableSubscriber instance(
      JsonStreamIterable jsonStreamWrapper,
      AsyncResponse asyncResponse,
      ChunkedOutputFactory chunkedOutputFactory,
      Duration maxDuration,
      Duration gracePeriod,
      ScheduledExecutorService executorService) {
    return new ResponseFlowableSubscriber(
        jsonStreamWrapper,
        asyncResponse,
        chunkedOutputFactory,
        maxDuration,
        gracePeriod,
        executorService);
  }

  @Override
  public void onSubscribe(@NonNull Subscription s) {
    this.subscription = s;
    this.subscription.request(1);
  }

  @Override
  public void onNext(ResultOrError t) {
    try {
      writeResult(t);
      subscription.request(1);
    } catch (Throwable e) {
      log.error("Encounter error onNext", e);
      close();
    }
  }

  @Override
  public void onError(Throwable t) {
    writeError(t);
    close();
  }

  @Override
  public void onComplete() {
    close();
  }

  public void writeError(Throwable error) {
    try {
      sink.write(ResultOrError.error(EXCEPTION_MAPPER.toErrorResponse(error)));
    } catch (Throwable e) {
      log.debug("Error when writing error result to response channel", e);
    }
  }

  public void close() {
    if (subscription != null) {
      subscription.cancel();
    }

    try {
      source.close();
    } catch (Throwable e) {
      log.debug("Error closing input", e);
    }

    try {
      sink.close();
    } catch (Throwable e) {
      log.debug("Error closing output", e);
    }
  }

  public void writeResult(ResultOrError r) {
    try {
      sink.write(r);
    } catch (Throwable e) {
      log.debug("Error when writing streaming result to response channel", e);
    }
  }

  private void scheduleConnectionCloseOnOverMaxDuration(ScheduledExecutorService executorService) {
    if (this.gracePeriod.isZero() || this.gracePeriod.isNegative()) {
      executorService.schedule(
          () -> {
            writeResult(
                ResultOrError.error(
                    EXCEPTION_MAPPER.toErrorResponse(
                        new StatusCodeException(
                            Status.REQUEST_TIMEOUT,
                            "Streaming connection open for longer than allowed",
                            "Connection will be closed."))));
            this.close();
          },
          this.maxDuration.toMillis(),
          TimeUnit.MILLISECONDS);
    } else {
      executorService.schedule(
          () ->
              writeResult(
                  ResultOrError.error(
                      EXCEPTION_MAPPER.toErrorResponse(
                          new StatusCodeException(
                              Status.REQUEST_TIMEOUT,
                              "Streaming connection open for longer than allowed",
                              "Connection will be closed.")))),
          this.maxDuration.toMillis(),
          TimeUnit.MILLISECONDS);
      executorService.schedule(
          this::close, this.maxDuration.toMillis() + gracePeriod.toMillis(), TimeUnit.MILLISECONDS);
    }
  }
}
