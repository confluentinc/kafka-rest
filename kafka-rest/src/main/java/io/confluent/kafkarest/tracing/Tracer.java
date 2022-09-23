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

package io.confluent.kafkarest.tracing;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO ddimitrov Should we make this injectable and provided to all the places from which we want
//  to trace? On one hand making it injectable is the idiomatic way to make it a statically
//  pluggable feature, on the other hand keeping it a static utility is the cheapest way to
//  implement tracing.
/**
 * Used for tracing requests when Kafka REST request tracing is enabled.
 *
 * <p>"Request tracing" here includes tracing across both the request and the response handling.
 */
public final class Tracer {

  public static final String TRACE_ID_PROPERTY_NAME = "io.confluent.kafkarest.tracing.traceId";

  private static final Logger logger = LoggerFactory.getLogger(Tracer.class);

  private static final LoadingCache<UUID, AtomicInteger> knownTraceIds =
      CacheBuilder.newBuilder()
          .maximumSize(1_000_000)
          .expireAfterAccess(Duration.ofMinutes(1))
          .build(CacheLoader.from(key -> new AtomicInteger(0)));

  /**
   * Makes the given message the next trace link in the trace chain of the given {@code traceId}.
   *
   * @param traceId The ID identifying the trace chain of the request which is being traced.
   * @param message The next message to be traced and added to the trace chain of some request. It
   *     should contain details for the corresponding step of the request/response handling. It can
   *     be a template that can be populated with the arguments provided via the {@code args}
   *     parameter.
   * @param args Varargs for the {@code message} parameter in case it refers to a String template.
   *     Should be null if {@code message} does not refer to a String template.
   */
  public static void trace(UUID traceId, String message, Object... args) {
    if (traceId != null && logger.isTraceEnabled()) {
      long timestampNs = System.nanoTime();
      AtomicInteger traceCounter = knownTraceIds.getUnchecked(traceId);
      String traceMessage = args == null ? message : String.format(message, args);
      logger.trace(
          "[Trace #{} for traceId {} @ {} ns]: {}",
          traceCounter.incrementAndGet(),
          traceId,
          timestampNs,
          traceMessage);
    }
  }

  public static void cleanup(UUID traceId) {
    knownTraceIds.invalidate(traceId);
  }

  public static boolean isEnabled() {
    return logger.isTraceEnabled();
  }
}
