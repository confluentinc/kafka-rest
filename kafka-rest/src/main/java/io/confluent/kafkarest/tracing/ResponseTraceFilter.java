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
import java.lang.reflect.Method;
import java.util.UUID;
import javax.annotation.Priority;
import javax.ws.rs.Priorities;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.container.ContainerResponseFilter;
import javax.ws.rs.container.ResourceInfo;
import javax.ws.rs.core.Context;

/**
 * Traces after the response handling when Kafka REST request tracing is enabled. Uses a priority
 * even higher than {@link Priorities#AUTHENTICATION} in order to be triggered after all the other
 * response filters have been triggered.
 */
@Priority(Priorities.AUTHENTICATION - 1000)
public class ResponseTraceFilter implements ContainerResponseFilter {

  private static final String RESPONSE_TRACE_TEMPLATE =
      "Response with status code %d and Content-Length %d is being returned";

  @Context private ResourceInfo matchedResource;

  @Override
  public void filter(
      ContainerRequestContext requestContext, ContainerResponseContext responseContext)
      throws IOException {
    UUID traceId = (UUID) requestContext.getProperty(Tracer.TRACE_ID_PROPERTY_NAME);
    if (traceId == null) {
      throw new IllegalStateException("traceId cannot be null if request tracing has been enabled");
    }
    Tracer.trace(
        traceId, RESPONSE_TRACE_TEMPLATE, responseContext.getStatus(), responseContext.getLength());
    Method matchedMethod = matchedResource.getResourceMethod();
    if (matchedMethod != null && !matchedMethod.toString().contains("ProduceAction")) {
      Tracer.cleanup(traceId);
    }
  }
}
