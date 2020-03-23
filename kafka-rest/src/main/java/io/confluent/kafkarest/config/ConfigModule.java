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

package io.confluent.kafkarest.config;

import io.confluent.kafkarest.KafkaRestConfig;
import io.confluent.rest.RestConfig;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import javax.inject.Qualifier;
import org.glassfish.hk2.api.AnnotationLiteral;
import org.glassfish.hk2.api.TypeLiteral;
import org.glassfish.hk2.utilities.binding.AbstractBinder;

/**
 * A module to populate the injector with the configurations passed to this application.
 *
 * <p>In addition to {@link KafkaRestConfig}, which contains all configurations, individual
 * configurations are also exposed, on a need-to-know basis.</p>
 */
public final class ConfigModule extends AbstractBinder {

  private final KafkaRestConfig config;

  public ConfigModule(KafkaRestConfig config) {
    this.config = Objects.requireNonNull(config);
  }

  @Override
  protected void configure() {
    bind(config).to(KafkaRestConfig.class);

    // Keep this list alphabetically sorted.
    bind(config.getList(KafkaRestConfig.ADVERTISED_LISTENERS_CONFIG))
        .qualifiedBy(new AdvertisedListenersConfigImpl())
        .to(new TypeLiteral<List<String>>() { });

    bind(config.getString(KafkaRestConfig.CRN_AUTHORITY_CONFIG))
        .qualifiedBy(new CrnAuthorityConfigImpl())
        .to(String.class);

    bind(config.getString(KafkaRestConfig.HOST_NAME_CONFIG))
        .qualifiedBy(new HostNameConfigImpl())
        .to(String.class);

    bind(config.getList(RestConfig.LISTENERS_CONFIG))
        .qualifiedBy(new ListenersConfigImpl())
        .to(new TypeLiteral<List<String>>() { });

    bind(config.getInt(KafkaRestConfig.NON_BLOCKING_EXECUTOR_CORE_POOL_SIZE_CONFIG))
        .qualifiedBy(new NonBlockingExecutorCorePoolSizeConfigImpl())
        .to(Integer.class);

    bind(config.getInt(KafkaRestConfig.NON_BLOCKING_EXECUTOR_MAX_POOL_SIZE_CONFIG))
        .qualifiedBy(new NonBlockingExecutorMaxPoolSizeConfigImpl())
        .to(Integer.class);

    bind(
        Duration.ofMillis(
            config.getLong(KafkaRestConfig.NON_BLOCKING_EXECUTOR_KEEP_ALIVE_MS_CONFIG)))
        .qualifiedBy(new NonBlockingExecutorKeepAliveConfigImpl())
        .to(Duration.class);

    bind(config.getInt(RestConfig.PORT_CONFIG))
        .qualifiedBy(new PortConfigImpl())
        .to(Integer.class);
  }

  @Qualifier
  @Retention(RetentionPolicy.RUNTIME)
  @Target({ ElementType.TYPE, ElementType.METHOD, ElementType.FIELD, ElementType.PARAMETER })
  public @interface AdvertisedListenersConfig {
  }

  private static final class AdvertisedListenersConfigImpl
      extends AnnotationLiteral<AdvertisedListenersConfig> implements AdvertisedListenersConfig {
  }

  @Qualifier
  @Retention(RetentionPolicy.RUNTIME)
  @Target({ ElementType.TYPE, ElementType.METHOD, ElementType.FIELD, ElementType.PARAMETER })
  public @interface CrnAuthorityConfig {
  }

  private static final class CrnAuthorityConfigImpl
      extends AnnotationLiteral<CrnAuthorityConfig> implements CrnAuthorityConfig {
  }

  @Qualifier
  @Retention(RetentionPolicy.RUNTIME)
  @Target({ ElementType.TYPE, ElementType.METHOD, ElementType.FIELD, ElementType.PARAMETER })
  @Deprecated
  public @interface HostNameConfig {
  }

  private static final class HostNameConfigImpl
      extends AnnotationLiteral<HostNameConfig> implements HostNameConfig {
  }

  @Qualifier
  @Retention(RetentionPolicy.RUNTIME)
  @Target({ ElementType.TYPE, ElementType.METHOD, ElementType.FIELD, ElementType.PARAMETER })
  public @interface ListenersConfig {
  }

  private static final class ListenersConfigImpl
      extends AnnotationLiteral<ListenersConfig> implements ListenersConfig {
  }

  @Qualifier
  @Retention(RetentionPolicy.RUNTIME)
  @Target({ ElementType.TYPE, ElementType.METHOD, ElementType.FIELD, ElementType.PARAMETER })
  public @interface NonBlockingExecutorCorePoolSizeConfig {
  }

  private static final class NonBlockingExecutorCorePoolSizeConfigImpl
      extends AnnotationLiteral<ListenersConfig> implements NonBlockingExecutorCorePoolSizeConfig {
  }

  @Qualifier
  @Retention(RetentionPolicy.RUNTIME)
  @Target({ ElementType.TYPE, ElementType.METHOD, ElementType.FIELD, ElementType.PARAMETER })
  public @interface NonBlockingExecutorMaxPoolSizeConfig {
  }

  private static final class NonBlockingExecutorMaxPoolSizeConfigImpl
      extends AnnotationLiteral<ListenersConfig> implements NonBlockingExecutorMaxPoolSizeConfig {
  }

  @Qualifier
  @Retention(RetentionPolicy.RUNTIME)
  @Target({ ElementType.TYPE, ElementType.METHOD, ElementType.FIELD, ElementType.PARAMETER })
  public @interface NonBlockingExecutorKeepAliveConfig {
  }

  private static final class NonBlockingExecutorKeepAliveConfigImpl
      extends AnnotationLiteral<ListenersConfig> implements NonBlockingExecutorKeepAliveConfig {
  }

  @Qualifier
  @Retention(RetentionPolicy.RUNTIME)
  @Target({ ElementType.TYPE, ElementType.METHOD, ElementType.FIELD, ElementType.PARAMETER })
  @Deprecated
  public @interface PortConfig {
  }

  private static final class PortConfigImpl
      extends AnnotationLiteral<PortConfig> implements PortConfig {
  }
}
