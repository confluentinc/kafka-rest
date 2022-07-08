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
import java.time.Duration;
import javax.inject.Inject;

public final class StreamingResponseFactory {

  private final ChunkedOutputFactory chunkedOutputFactory;
  private final Duration maxDuration;
  private final Duration gracePeriod;

  @Inject
  public StreamingResponseFactory(
      ChunkedOutputFactory chunkedOutputFactory,
      @StreamingMaxConnectionDurationConfig Duration maxDuration,
      @StreamingMaxConnectionGracePeriod Duration gracePeriod) {
    this.chunkedOutputFactory = requireNonNull(chunkedOutputFactory);
    this.maxDuration = maxDuration;
    this.gracePeriod = gracePeriod;
  }

  public <T> StreamingResponse<T> from(JsonStream<T> inputStream) {
    return StreamingResponse.from(inputStream, chunkedOutputFactory, maxDuration, gracePeriod);
  }
}
