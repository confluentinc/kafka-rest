/**
 * Copyright 2015 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.kafkarest.resources;

import java.util.List;
import java.util.Map.Entry;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.container.ContainerResponseFilter;

/**
 * Simply reflects any request headers starting with x- (or X-) into the response. This is initially
 * to allow clients that need to manually correlate requests with responses to inject a header,
 * usually something like X-Correlation-Id, to facilitate matching.
 */
public class XHeaderReflectingResponseFilter implements ContainerResponseFilter {

  @Override
  public void filter(final ContainerRequestContext requestContext,
                     final ContainerResponseContext responseContext) {
    for (Entry<String, List<String>> header : requestContext.getHeaders().entrySet()) {
      if (header.getKey().toLowerCase().trim().startsWith("x-")) {
        responseContext.getHeaders().add(header.getKey(), header.getValue());
      }
    }
  }
}
