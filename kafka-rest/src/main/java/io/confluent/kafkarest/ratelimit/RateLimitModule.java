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

package io.confluent.kafkarest.ratelimit;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import javax.inject.Qualifier;
import javax.inject.Singleton;
import org.glassfish.hk2.api.AnnotationLiteral;
import org.glassfish.hk2.api.PerLookup;
import org.glassfish.hk2.utilities.binding.AbstractBinder;

/**
 * A module to configure bindings for {@link FixedCostRateLimiter} and {@link RequestRateLimiter}.
 */
public final class RateLimitModule extends AbstractBinder {

  @Override
  protected void configure() {
    bindFactory(RequestRateLimiterGenericFactory.class)
        .qualifiedBy(new RequestRateLimiterGenericImpl())
        .to(RequestRateLimiter.class)
        .in(Singleton.class);

    bindFactory(RequestRateLimiterProduceCountFactory.class)
        .qualifiedBy(new ProduceRateLimiterCountImpl())
        .to(RequestRateLimiter.class)
        .in(PerLookup.class);

    bindFactory(RequestRateLimiterProduceBytesFactory.class)
        .qualifiedBy(new ProduceRateLimiterBytesImpl())
        .to(RequestRateLimiter.class)
        .in(PerLookup.class);

    bindFactory(RequestRateLimiterProduceCountGlobalFactory.class)
        .qualifiedBy(new ProduceRateLimiterCountGlobalImpl())
        .to(RequestRateLimiter.class)
        .in(Singleton.class);

    bindFactory(RequestRateLimiterProduceBytesGlobalFactory.class)
        .qualifiedBy(new ProduceRateLimiterBytesGlobalImpl())
        .to(RequestRateLimiter.class)
        .in(Singleton.class);
  }

  @Qualifier
  @Retention(RetentionPolicy.RUNTIME)
  @Target({ElementType.TYPE, ElementType.METHOD, ElementType.FIELD, ElementType.PARAMETER})
  public @interface ProduceRateLimiterCount {}

  private static final class ProduceRateLimiterCountImpl
      extends AnnotationLiteral<ProduceRateLimiterCount> implements ProduceRateLimiterCount {}

  @Qualifier
  @Retention(RetentionPolicy.RUNTIME)
  @Target({ElementType.TYPE, ElementType.METHOD, ElementType.FIELD, ElementType.PARAMETER})
  public @interface ProduceRateLimiterBytes {}

  private static final class ProduceRateLimiterBytesImpl
      extends AnnotationLiteral<ProduceRateLimiterBytes> implements ProduceRateLimiterBytes {}

  @Qualifier
  @Retention(RetentionPolicy.RUNTIME)
  @Target({ElementType.TYPE, ElementType.METHOD, ElementType.FIELD, ElementType.PARAMETER})
  public @interface ProduceRateLimiterCountGlobal {}

  private static final class ProduceRateLimiterCountGlobalImpl
      extends AnnotationLiteral<ProduceRateLimiterCountGlobal>
      implements ProduceRateLimiterCountGlobal {}

  @Qualifier
  @Retention(RetentionPolicy.RUNTIME)
  @Target({ElementType.TYPE, ElementType.METHOD, ElementType.FIELD, ElementType.PARAMETER})
  public @interface ProduceRateLimiterBytesGlobal {}

  private static final class ProduceRateLimiterBytesGlobalImpl
      extends AnnotationLiteral<ProduceRateLimiterBytesGlobal>
      implements ProduceRateLimiterBytesGlobal {}

  @Qualifier
  @Retention(RetentionPolicy.RUNTIME)
  @Target({ElementType.TYPE, ElementType.METHOD, ElementType.FIELD, ElementType.PARAMETER})
  public @interface RequestRateLimiterGeneric {}

  private static final class RequestRateLimiterGenericImpl
      extends AnnotationLiteral<RequestRateLimiterGeneric> implements RequestRateLimiterGeneric {}
}
