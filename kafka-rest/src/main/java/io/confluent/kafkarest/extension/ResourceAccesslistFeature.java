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

package io.confluent.kafkarest.extension;

import static java.util.Objects.requireNonNull;

import io.confluent.kafkarest.config.ConfigModule.ApiEndpointsAllowlistConfig;
import io.confluent.kafkarest.config.ConfigModule.ApiEndpointsBlocklistConfig;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.Set;
import javax.annotation.Priority;
import javax.inject.Inject;
import javax.ws.rs.HttpMethod;
import javax.ws.rs.NotAllowedException;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.DynamicFeature;
import javax.ws.rs.container.ResourceInfo;
import javax.ws.rs.core.FeatureContext;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import org.glassfish.jersey.server.wadl.processor.OptionsMethodProcessor;

/**
 * A feature that controls access to endpoints based on allowlist and blocklist configs.
 *
 * <p>Endpoints are identified using the {@link ResourceName} annotation that can be applied to an
 * endpoint's class and methods - the corresponding {@link ResourceName#value() annotation value} is
 * the identifier that can be populated in allowlists and blocklists.
 *
 * <p>If an allowlist has been configured and an endpoint's identifier <b>is not present</b> in the
 * allowlist, then the endpoint will be disabled. Similarly if a blocklist has been configured and
 * an endpoint's identifier <b>is present</b> in the blocklist, then the endpoint will be disabled.
 * Both an allowlist and a blocklist can be present at the same time.
 *
 * <ul>
 *   <li><i>Disabling an endpoint by class also disables all its method endpoints.</i>
 * </ul>
 *
 * <p>A disabled endpoint returns HTTP 404 Not Found for GET requests, and HTTP 405 Method Not
 * Allowed for everything else.
 */
public final class ResourceAccesslistFeature implements DynamicFeature {

  private final Set<String> apiEndpointsBlocklistConfig;
  private final Set<String> apiEndpointsAllowlistConfig;

  @Inject
  ResourceAccesslistFeature(
      @ApiEndpointsAllowlistConfig Set<String> apiEndpointsAllowlistConfig,
      @ApiEndpointsBlocklistConfig Set<String> apiEndpointsBlocklistConfig) {
    this.apiEndpointsAllowlistConfig = requireNonNull(apiEndpointsAllowlistConfig);
    this.apiEndpointsBlocklistConfig = requireNonNull(apiEndpointsBlocklistConfig);
  }

  @Override
  public void configure(ResourceInfo resourceInfo, FeatureContext context) {
    if (isOptionsProcessor(resourceInfo)) {
      // Always allow OPTIONS requests. We don't have a good mechanism for blocking OPTIONS requests
      // in specific endpoints because the default OPTIONS handler handles requests for all
      // endpoints.
      return;
    }

    boolean blocked =
        isBlockedByAccesslist(resourceInfo, apiEndpointsAllowlistConfig, true)
            || isBlockedByAccesslist(resourceInfo, apiEndpointsBlocklistConfig, false);

    if (blocked) {
      context.register(ThrowingFilter.class);
    }
  }

  private static boolean isOptionsProcessor(ResourceInfo resourceInfo) {
    return resourceInfo.getResourceClass() != null
        && resourceInfo.getResourceClass().getEnclosingClass() != null
        && resourceInfo.getResourceClass().getEnclosingClass().equals(OptionsMethodProcessor.class);
  }

  private boolean isBlockedByAccesslist(
      ResourceInfo resourceInfo, Set<String> accesslist, boolean isAccesslistAllowlist) {
    // If the corresponding accesslist (not only the blocklist, but also the allowlist) is empty,
    // it cannot block an endpoint.
    if (accesslist.isEmpty()) {
      return false;
    }

    // When using an allowlist, endpoints are considered blocked unless explicitly allowed. And
    // conversely, when using a blocklist, endpoints are considered allowed unless explicitly
    // blocked.
    boolean blocked = isAccesslistAllowlist;

    ResourceName resourceClassName =
        resourceInfo.getResourceClass().getAnnotation(ResourceName.class);
    if (resourceClassName != null && accesslist.contains(resourceClassName.value())) {
      blocked = !isAccesslistAllowlist;
    }

    ResourceName resourceMethodName =
        resourceInfo.getResourceMethod().getAnnotation(ResourceName.class);
    if (resourceMethodName != null && accesslist.contains(resourceMethodName.value())) {
      blocked = !isAccesslistAllowlist;
    }

    return blocked;
  }

  /** A name by which a resource class/method can be referenced. */
  @Target({ElementType.METHOD, ElementType.TYPE})
  @Retention(RetentionPolicy.RUNTIME)
  public @interface ResourceName {

    /** The resource class/method name. */
    String value();
  }

  @Priority(MorePriorities.PRE_AUTHENTICATION)
  private static final class ThrowingFilter implements ContainerRequestFilter {

    @Override
    public void filter(ContainerRequestContext context) {
      if (HttpMethod.GET.equals(context.getMethod())) {
        throw new NotFoundException();
      } else {
        throw new NotAllowedException(Response.status(Status.METHOD_NOT_ALLOWED).build());
      }
    }
  }
}
