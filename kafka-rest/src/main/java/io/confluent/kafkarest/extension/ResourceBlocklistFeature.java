package io.confluent.kafkarest.extension;

import static java.util.Objects.requireNonNull;

import io.confluent.kafkarest.config.ConfigModule.ApiEndpointsBlocklistConfig;
import io.confluent.kafkarest.exceptions.DisabledOperationException;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.Set;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.DynamicFeature;
import javax.ws.rs.container.ResourceInfo;
import javax.ws.rs.core.FeatureContext;
import javax.ws.rs.core.Response.Status;

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

    if (!blocked) {
      return;
    }

    if (resourceInfo.getResourceMethod().getAnnotation(GET.class) != null) {
      context.register(new ThrowingFilter(Status.NOT_FOUND));
    } else {
      context.register(new ThrowingFilter(Status.METHOD_NOT_ALLOWED));
    }
  }

  @Target({ElementType.METHOD, ElementType.TYPE})
  @Retention(RetentionPolicy.RUNTIME)
  public @interface ResourceName {
    String value();
  }

  private static final class ThrowingFilter implements ContainerRequestFilter {
    private final Status status;

    private ThrowingFilter(Status status) {
      this.status = requireNonNull(status);
    }

    @Override
    public void filter(ContainerRequestContext context) {
      throw new DisabledOperationException(status);
    }
  }
}
