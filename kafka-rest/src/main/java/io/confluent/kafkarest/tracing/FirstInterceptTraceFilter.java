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

import java.io.IOException;
import java.util.UUID;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.PreMatching;

/**
 * Initializes tracing when Kafka REST request tracing is enabled. Triggered before the incoming
 * request has been matched (successfully or unsuccessfully) to a resource method.
 */
@PreMatching
public class FirstInterceptTraceFilter implements ContainerRequestFilter {

  private static final String FIRST_INTERCEPT_TRACE_TEMPLATE = "Received a %s request to %s";

  @Override
  public void filter(ContainerRequestContext requestContext) throws IOException {
    UUID traceId = UUID.randomUUID();
    requestContext.setProperty(Tracer.TRACE_ID_PROPERTY_NAME, traceId);
    Tracer.trace(
        traceId,
        FIRST_INTERCEPT_TRACE_TEMPLATE,
        requestContext.getMethod(),
        requestContext.getUriInfo().getAbsolutePath());
  }
}
