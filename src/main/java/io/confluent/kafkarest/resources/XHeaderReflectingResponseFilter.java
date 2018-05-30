/**
 * Copyright 2015 Confluent Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.kafkarest.resources;

import io.confluent.kafkarest.KafkaRestContext;

import java.util.List;
import java.util.Map.Entry;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.container.ContainerResponseFilter;
import javax.ws.rs.ext.Provider;

import static io.confluent.kafkarest.KafkaRestConfig.REFLECT_XHEADERS_CONFIG;

/**
 * Simply reflects any request headers starting with x- (or X-) into the response. This is initially
 * to allow clients that need to manually correlate requests with responses to inject a header,
 * usually something like X-Correlation-Id, to facilitate matching.
 */
@Provider
public class XHeaderReflectingResponseFilter implements ContainerResponseFilter {

  private boolean enabled;

  /**
   * Reads the given context to extract the 'reflect.xheaders' property and assess it for
   * true or false. Default is 'false' if it's not specified or context is null.
   */
  public XHeaderReflectingResponseFilter(KafkaRestContext context) {
    if (context == null) {
      enabled = false;
    } else {
      final Object property =
          context.getConfig().getOriginalProperties().get(REFLECT_XHEADERS_CONFIG);
      enabled = property instanceof Boolean && (boolean) property;
    }

  }

  @Override
  public void filter(final ContainerRequestContext requestContext,
                     final ContainerResponseContext responseContext) {
    if (enabled) {
      for (Entry<String, List<String>> header : requestContext.getHeaders().entrySet()) {
        if (header.getKey() != null
            && header.getKey().toLowerCase().trim().startsWith("x-")) {
          List<String> valueList = header.getValue();
          for (String value : valueList) {
            responseContext.getHeaders().add(header.getKey(), value);
          }
        }
      }
    }
  }
}
