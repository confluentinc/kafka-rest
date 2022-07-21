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

package io.confluent.kafkarest.integration.accesslist;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.confluent.kafkarest.KafkaRestConfig;
import io.confluent.kafkarest.extension.RestResourceExtension;
import io.confluent.kafkarest.integration.ClusterTestHarness;
import java.util.Collections;
import java.util.Properties;
import javax.annotation.Priority;
import javax.ws.rs.Priorities;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.core.Configurable;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import org.junit.jupiter.api.Test;

public class ResourceDiscoverabilityTest extends ClusterTestHarness {

  @Priority(Priorities.AUTHENTICATION)
  private static final class NoAuthFilter implements ContainerRequestFilter {
    @Override
    public void filter(ContainerRequestContext containerRequestContext) {
      containerRequestContext.abortWith(Response.status(Status.UNAUTHORIZED).build());
    }
  }

  public static final class NoAuthExtension implements RestResourceExtension {
    @Override
    public void register(Configurable<?> config, KafkaRestConfig appConfig) {
      config.register(NoAuthFilter.class);
    }

    @Override
    public void clean() {}
  }

  @Override
  protected void overrideKafkaRestConfigs(Properties restProperties) {
    restProperties.put(KafkaRestConfig.API_ENDPOINTS_BLOCKLIST_CONFIG, "api.v3.clusters.get");
    restProperties.put(
        KafkaRestConfig.KAFKA_REST_RESOURCE_EXTENSION_CONFIG,
        Collections.singletonList(NoAuthExtension.class));
  }

  @Test
  public void testDiscoverability() {
    // Trying to access a non-blocklisted resource should result in an HTTP 401, because of the
    // custom authentication filter we've registered in the test.
    Response getResponse = request("/v3/clusters/").accept(MediaType.APPLICATION_JSON).get();
    assertEquals(Status.UNAUTHORIZED.getStatusCode(), getResponse.getStatus());

    // Trying to GET (as opposed to POST, DELETE, etc.) a blocklisted resource should result in an
    // HTTP 404, because the blocklist feature should be triggered before authentication-priority
    // filters.
    Response getBlocklistedResponse =
        request("/v3/clusters/" + getClusterId()).accept(MediaType.APPLICATION_JSON).get();
    assertEquals(Status.NOT_FOUND.getStatusCode(), getBlocklistedResponse.getStatus());

    // Trying to access a non-existing resource should result in an HTTP 404, as expected.
    Response getNonExistingResponse = request("/v3/foo").accept(MediaType.APPLICATION_JSON).get();
    assertEquals(Status.NOT_FOUND.getStatusCode(), getNonExistingResponse.getStatus());
  }
}
