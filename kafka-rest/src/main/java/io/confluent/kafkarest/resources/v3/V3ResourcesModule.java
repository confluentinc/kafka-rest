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

package io.confluent.kafkarest.resources.v3;

import io.confluent.kafkarest.resources.RateLimiter;
import io.confluent.kafkarest.response.ChunkedOutputFactory;
import io.confluent.kafkarest.response.StreamingResponseFactory;
import java.time.Clock;
import javax.inject.Singleton;
import org.glassfish.hk2.utilities.binding.AbstractBinder;

public final class V3ResourcesModule extends AbstractBinder {

  @Override
  protected void configure() {
    bindAsContract(RateLimiter.class).in(Singleton.class);
    bindAsContract(ChunkedOutputFactory.class);
    bindAsContract(StreamingResponseFactory.class);
    bind(Clock.systemUTC()).to(Clock.class);
  }
}
