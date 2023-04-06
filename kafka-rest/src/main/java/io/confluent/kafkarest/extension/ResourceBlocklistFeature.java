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

/**
 * A feature that disables endpoints based on the {@link ResourceName} annotation.
 *
 * <p>If an endpoint's method or class is annotated with {@code ResourceName}, and its {@link
 * ResourceName#value() name} is present in the endpoints blacklist, then the endpoint will be
 * disabled.
 *
 * <p>A disabled endpoint returns HTTP 404 Not Found for GET requests, and HTTP 405 Method Not
 * Allowed for everything else.
 */
public final class ResourceBlocklistFeature implements DynamicFeature {

  private final Set<String> apiEndpointsBlocklistConfig;

  @Inject
  ResourceBlocklistFeature(@ApiEndpointsBlocklistConfig Set<String> apiEndpointsBlocklistConfig) {
    this.apiEndpointsBlocklistConfig = requireNonNull(apiEndpointsBlocklistConfig);
  }

  @Override
  public void configure(ResourceInfo resourceInfo, FeatureContext context) {
    boolean blocked = false;

    ResourceName resourceClassName =
        resourceInfo.getResourceClass().getAnnotation(ResourceName.class);
    if (resourceClassName != null
        && apiEndpointsBlocklistConfig.contains(resourceClassName.value())) {
      blocked = true;
    }

    ResourceName resourceMethodName =
        resourceInfo.getResourceMethod().getAnnotation(ResourceName.class);
    if (resourceMethodName != null
        && apiEndpointsBlocklistConfig.contains(resourceMethodName.value())) {
      blocked = true;
    }

    if (blocked) {
      context.register(ThrowingFilter.class);
    }
  }

  /**
   * A name by which a resource class/method can be referenced.
   */
  @Target({ElementType.METHOD, ElementType.TYPE})
  @Retention(RetentionPolicy.RUNTIME)
  public @interface ResourceName {

    /**
     * The resource class/method name.
     */
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
